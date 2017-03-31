/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reforest.rf

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, TaskContext}
import reforest.TypeInfo
import reforest.data.{DataOnWorker, StaticData}
import reforest.dataTree.{Cut, TreeNode}
import reforest.rf.split.RFSplitterManager
import reforest.util.{BiMap, GCInstrumented, IncrementalMean}

import scala.collection.Map
import scala.reflect.ClassTag

class RFTreeGeneration[T: ClassTag, U: ClassTag](@transient sc: SparkContext,
                                                 property: Broadcast[RFProperty],
                                                 typeInfo: Broadcast[TypeInfo[T]],
                                                 typeInfoWorking: Broadcast[TypeInfo[U]],
                                                 categoricalFeatures: Broadcast[RFCategoryInfo],
                                                 splitter: Broadcast[RFSplitterManager[T, U]],
                                                 strategy: Broadcast[RFStrategy[T, U]],
                                                 numTrees: Int,
                                                 macroIteration : Int) extends Serializable {

  val entropy = sc.broadcast(new RFEntropy[T, U](typeInfo, typeInfoWorking))

  def findBestCutEntropy(nodeIdTree: Int,
                         matrixRow: Array[Int],
                         idTOid: BiMap[(Int, Int), Int],
                         featureIdMap: Map[(Int, Int), Array[Int]],
                         depth: Int): ((Int, Int), Cut[T, U]) = {
    var bestSplit: Option[Cut[T, U]] = Option.empty
    val (treeId, nodeId) = idTOid(nodeIdTree)
    val featurePerNode = featureIdMap((treeId, nodeId))

    var featurePosition = 0
    while (featurePosition < featurePerNode.length) {
      val featureId = featurePerNode(featurePosition)
      val offset = DataOnWorker.getOffset(nodeIdTree, featurePosition)
      val rowSubset = rowSubsetPrepare(matrixRow, offset._1, offset._2, property.value.numClasses)
      val g = if (categoricalFeatures.value.isCategorical(featureId)) entropy.value.getBestSplitCategorical(rowSubset, featureId, splitter.value.getSplitter(macroIteration , treeId), depth, property.value.maxDepth, property.value.numClasses)
      else entropy.value.getBestSplit(rowSubset, featureId, splitter.value.getSplitter(macroIteration, treeId), depth, property.value.maxDepth, property.value.numClasses)
      if (bestSplit.isEmpty || g.stats > bestSplit.get.stats) bestSplit = Some(g)
      featurePosition += 1
    }

    ((treeId, nodeId), bestSplit.get)
  }

  def findBestCutEntropy(args: (Int, Array[Int]),
                         idTOid: BiMap[(Int, Int), Int],
                         featureIdMap: Map[(Int, Int), Array[Int]],
                         depth: Int): ((Int, Int), Cut[T, U]) = {
    findBestCutEntropy(args._1, args._2, idTOid, featureIdMap, depth)
  }

  def rowSubsetPrepare(row: Array[Int], start: Int, end: Int, numClasses: Int): Array[Array[Int]] = {
    val toReturn = Array.tabulate((end - start) / numClasses)(_ => new Array[Int](numClasses))

    var count = 0
    while (count < (end - start)) {
      toReturn(count / numClasses)(count % numClasses) = row(start + count)
      count += 1
    }

    toReturn
  }

  def mapPartitionsPointToMatrix(it: Iterator[StaticData[U]],
                                 featureMap: Broadcast[Map[(Int, Int), Array[Int]]],
                                 iteration: Int,
                                 iterationNum: Int,
                                 rootArray: Broadcast[Array[TreeNode[T, U]]],
                                 idTOid: Broadcast[BiMap[(Int, Int), Int]]): Unit = {

    it.foreach { case data =>
      var treeId = 0
      while (treeId < numTrees) {
        val weight = data.getBagging(treeId)
        if (weight > 0 && treeId % iterationNum == iteration) {
          val point = data.getWorkingData(treeId)
          val nodeIdOption = rootArray.value(treeId).getCurrentNodeId(point, typeInfoWorking.value)
          if (nodeIdOption.isDefined) {
            val nodeId = nodeIdOption.get
            val idx = idTOid.value((treeId, nodeId))
            if (idx < DataOnWorker.arrayLength) {
              val idArray = DataOnWorker.array(idx)
              val validFeatures: Array[Int] = featureMap.value.get((treeId, nodeId)).getOrElse(Array.empty)
              var featurePosition = 0
              val featurePositionArray = DataOnWorker.getOffset(idx)
              idArray.synchronized({
                while (featurePosition < validFeatures.length) {
                  idArray(DataOnWorker.getPositionOffset(featurePositionArray(featurePosition), data.getLabel, typeInfoWorking.value.toInt(point(validFeatures(featurePosition))), property.value.numClasses)) += weight
                  featurePosition += 1
                }
              })
            }
          }
        }
        treeId += 1
      }
    }
  }

  def reduceMatrix(a: Array[Int], b: Array[Int]): Array[Int] = {
    var i = 0
    while (i < a.length) {
      a(i) += b(i)
      i += 1
    }

    a
  }

  def findBestCut(sc: SparkContext,
                  dataIndex: RDD[StaticData[U]],
                  featureSelected: Map[(Int, Int), Array[Int]],
                  rootArrayArg: Array[TreeNode[T, U]],
                  depth: Int,
                  instrumented: Broadcast[GCInstrumented]) = {

    if (featureSelected.isEmpty) {
      Array[((Int, Int), Cut[T, U])]()
    } else {
      val iterationNumber = Math.ceil(featureSelected.size.toDouble / property.value.maxNodesConcurrent).toInt
      var iteration = 0

      val rootArray = sc.broadcast(rootArrayArg)

      var toReturn: Array[((Int, Int), Cut[T, U])] = Array.empty

      instrumented.value.start()

      while (iteration < iterationNumber) {
        val fMapTmp = featureSelected.filter(t => t._1._1 % iterationNumber == iteration).map(t => (t._1, t._2.sorted))
        val featureIdMap = sc.broadcast(fMapTmp)
        val idTOid = sc.broadcast(new BiMap(featureSelected.filter(t => t._1._1 % iterationNumber == iteration).toArray.map(t => t._1).sortBy(_._1).zipWithIndex.toMap))

        /* Matrix initialization */
        dataIndex.foreachPartition(t => {
          instrumented.value.gc(DataOnWorker.init(depth, iteration, iterationNumber, fMapTmp.size, property.value.numClasses, splitter, featureIdMap, idTOid))
        })

        /* Local information collection */
        dataIndex.foreachPartition(t => {
          instrumented.value.gc(mapPartitionsPointToMatrix(t, featureIdMap, iteration, iterationNumber, rootArray, idTOid))
        })

        /* Distributed Information Aggregation + Best Cut Finding */
        toReturn = toReturn ++ dataIndex.mapPartitions { case (it) =>
          instrumented.value.gc()
          if (TaskContext.getPartitionId() == DataOnWorker.initBy) {
            val toReturn = DataOnWorker.array.zipWithIndex.map(_.swap).iterator
            DataOnWorker.releaseMatrix
            toReturn
          } else
            Iterator()
        }.reduceByKey(reduceMatrix)
          .mapPartitions(nodeIdMatrix => {
            val toReturn = nodeIdMatrix.map(t => findBestCutEntropy(t, idTOid.value, featureIdMap.value, depth))
            instrumented.value.gc()
            toReturn
          })
          .collect()

        if (instrumented.value.valid)
          dataIndex.foreachPartition(t => instrumented.value.gc())

        iteration += 1
        featureIdMap.unpersist()
        idTOid.unpersist()
      }

      instrumented.value.stop()

      rootArray.unpersist()

      toReturn
    }
  }

  def updateForest(cut: Cut[T, U],
                   treeId: Int,
                   node: TreeNode[T, U],
                   featureSQRT: collection.mutable.Map[(Int, Int), Array[Int]]) = {
    val mean = new IncrementalMean

    if ((cut.left + cut.right) == 0) {
      node.label = cut.label
      node.isLeaf = true
    } else if (cut.notValid > 0) {
      val cutNotValid = cut.getNotValid(typeInfo.value, typeInfoWorking.value)

      if (cut.left > 0 && cut.right > 0) {
        node.split = Some(cutNotValid)

        val leftChild = node.getLeftChild()

        val rightChild = node.getRightChild()
        rightChild.split = Some(cut)

        val rightleftChild = rightChild.getLeftChild()
        val rightrightChild = rightChild.getRightChild()

        leftChild.label = Some(cut.labelNotValid.get)
        if (cut.labelNotValidOk == cut.notValid || TreeNode.indexToLevelCheck(leftChild.id) > property.value.maxDepth) {
          leftChild.isLeaf = true
        } else {
          featureSQRT((treeId, leftChild.id)) = strategy.value.getSQRTFeatures()
          mean.updateMean(cut.left)
        }

        if (cut.labelLeftOk == cut.left || TreeNode.indexToLevelCheck(rightleftChild.id) > property.value.maxDepth) {
          rightleftChild.isLeaf = true
          rightleftChild.label = Some(cut.labelLeft.get)
        } else {
          featureSQRT((treeId, rightleftChild.id)) = strategy.value.getSQRTFeatures()
          mean.updateMean(cut.right)
        }

        if (cut.labelRightOk == cut.right || TreeNode.indexToLevelCheck(rightrightChild.id) > property.value.maxDepth) {
          rightrightChild.isLeaf = true
          rightrightChild.label = Some(cut.labelRight.get)
        } else {
          featureSQRT((treeId, rightrightChild.id)) = strategy.value.getSQRTFeatures()
        }
      } else {
        node.split = Some(cutNotValid)

        val leftChild = node.getLeftChild()
        val rightChild = node.getRightChild()
        if (cut.labelLeftOk > cut.labelRightOk)
          rightChild.label = Some(cut.labelLeft.get)
        else rightChild.label = Some(cut.labelRight.get)
        rightChild.isLeaf = true

        leftChild.label = Some(cut.labelNotValid.get)
        if (cut.labelNotValidOk == cut.notValid || TreeNode.indexToLevelCheck(leftChild.id) > property.value.maxDepth) {
          leftChild.isLeaf = true
        } else {
          featureSQRT((treeId, leftChild.id)) = strategy.value.getSQRTFeatures()
          mean.updateMean(cut.left)
        }
      }
    } else {
      if (cut.left > 0 && cut.right > 0) {
        node.split = Some(cut)

        val leftChild = node.getLeftChild()
        val rightChild = node.getRightChild()

        leftChild.label = cut.labelLeft
        rightChild.label = cut.labelRight

        if (cut.labelLeftOk == cut.left || TreeNode.indexToLevelCheck(leftChild.id) > property.value.maxDepth) {
          leftChild.isLeaf = true
        } else {
          featureSQRT((treeId, leftChild.id)) = strategy.value.getSQRTFeatures()
          mean.updateMean(cut.left)
        }
        if (cut.labelRightOk == cut.right || TreeNode.indexToLevelCheck(rightChild.id) > property.value.maxDepth) {
          rightChild.isLeaf = true
        } else {
          featureSQRT((treeId, rightChild.id)) = strategy.value.getSQRTFeatures()
          mean.updateMean(cut.right)
        }
      } else {
        node.label = cut.label
        node.isLeaf = true
      }
    }

    mean.getMean
  }
}

