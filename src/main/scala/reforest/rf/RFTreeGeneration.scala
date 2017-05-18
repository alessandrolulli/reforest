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
import reforest.dataTree.{CutDetailed, Forest}
import reforest.rf.split.RFSplitterManager
import reforest.util.{BiMap, GCInstrumented, IncrementalMean}

import scala.collection.Map
import scala.reflect.ClassTag

class RFTreeGeneration[T: ClassTag, U: ClassTag](@transient private val sc: SparkContext,
                                                 property: Broadcast[RFProperty],
                                                 typeInfo: Broadcast[TypeInfo[T]],
                                                 typeInfoWorking: Broadcast[TypeInfo[U]],
                                                 categoricalFeatures: Broadcast[RFCategoryInfo],
                                                 splitter: Broadcast[RFSplitterManager[T, U]],
                                                 strategy: Broadcast[RFStrategy[T, U]],
                                                 numTrees: Int,
                                                 macroIteration: Int) extends Serializable {

  val entropy = sc.broadcast(new RFEntropy[T, U](typeInfo, typeInfoWorking))

  def findBestCutEntropy(nodeIdTree: Int,
                         matrixRow: Array[Int],
                         idTOid: BiMap[(Int, Int), Int],
                         featureIdMap: Map[(Int, Int), Array[Int]],
                         depth: Int): ((Int, Int), CutDetailed[T, U]) = {
    var bestSplit: Option[CutDetailed[T, U]] = Option.empty
    val (treeId, nodeId) = idTOid(nodeIdTree)
    val featurePerNode = featureIdMap((treeId, nodeId))

    var featurePosition = 0
    while (featurePosition < featurePerNode.length) {
      val featureId = featurePerNode(featurePosition)
      val offset = DataOnWorker.getOffset(nodeIdTree, featurePosition)
      val rowSubset = rowSubsetPrepare(matrixRow, offset._1, offset._2, property.value.numClasses)
      val g = if (categoricalFeatures.value.isCategorical(featureId)) entropy.value.getBestSplitCategorical(rowSubset, featureId, splitter.value.getSplitter(macroIteration, treeId), depth, property.value.maxDepth, property.value.numClasses)
      else entropy.value.getBestSplit(rowSubset, featureId, splitter.value.getSplitter(macroIteration, treeId), depth, property.value.maxDepth, property.value.numClasses)
      if (bestSplit.isEmpty || g.stats > bestSplit.get.stats) bestSplit = Some(g)
      featurePosition += 1
    }

    ((treeId, nodeId), bestSplit.get)
  }

  def findBestCutEntropy(args: (Int, Array[Int]),
                         idTOid: BiMap[(Int, Int), Int],
                         featureIdMap: Map[(Int, Int), Array[Int]],
                         depth: Int): ((Int, Int), CutDetailed[T, U]) = {
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

  def rowSubsetPrepareWithData(toReturn: Array[Array[Int]], row: Array[Int], start: Int, end: Int, numClasses: Int): Array[Array[Int]] = {
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
                                 forest: Broadcast[Forest[T, U]],
                                 idTOid: Broadcast[BiMap[(Int, Int), Int]]): Unit = {

    it.foreach { case data =>
      var treeId = 0
      while (treeId < numTrees) {
        val weight = data.getBagging(treeId)
        if (weight > 0 && treeId % iterationNum == iteration) {
          val point = data.getWorkingData(treeId)
          val nodeIdOption = forest.value.getCurrentNodeId(treeId, point, typeInfoWorking.value)
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
                  forestArg: Forest[T, U],
                  depth: Int,
                  instrumented: Broadcast[GCInstrumented]) = {

    if (featureSelected.isEmpty) {
      Array[((Int, Int), CutDetailed[T, U])]()
    } else {
      val iterationNumber = Math.ceil(featureSelected.size.toDouble / property.value.maxNodesConcurrent).toInt
      var iteration = 0

      val forestBC = sc.broadcast(forestArg)

      var toReturn: Array[((Int, Int), CutDetailed[T, U])] = Array.empty

      instrumented.value.start()

      while (iteration < iterationNumber) {
        val fMapTmp = featureSelected.filter(t => t._1._1 % iterationNumber == iteration).map(t => (t._1, t._2.sorted))
        val featureIdMap = sc.broadcast(fMapTmp)
        val idTOid = sc.broadcast(new BiMap(featureSelected.filter(t => t._1._1 % iterationNumber == iteration).toArray.map(t => t._1).sortBy(_._1).zipWithIndex.toMap))

        dataIndex.foreachPartition(t => {
          /* Matrix initialization */
          instrumented.value.gc(DataOnWorker.init(depth, iteration, iterationNumber, fMapTmp.size, property.value.numClasses, splitter, featureIdMap, idTOid))
          /* Local information collection */
          instrumented.value.gc(mapPartitionsPointToMatrix(t, featureIdMap, iteration, iterationNumber, forestBC, idTOid))
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

      forestBC.unpersist()

      toReturn
    }
  }

  def updateForest(cut: CutDetailed[T, U],
                   treeId: Int,
                   nodeId: Int,
                   forest: Forest[T, U],
                   featureSQRT: Option[collection.mutable.Map[(Int, Int), Array[Int]]] = Option.empty) = {
    val mean = new IncrementalMean

    if ((cut.left + cut.right) == 0) {
      if(cut.label.isDefined) {
        forest.setLabel(treeId, nodeId, cut.label.get)
      }
      forest.setLeaf(treeId, nodeId)
    } else if (cut.notValid > 0) {
      val cutNotValid = cut.getNotValid(typeInfo.value, typeInfoWorking.value)
      if(cut.label.isDefined) forest.setLabel(treeId, nodeId, cut.label.get)

      if (cut.left > 0 && cut.right > 0) {
        forest.setSplit(treeId, nodeId, cutNotValid)

        val leftChild = forest.getLeftChild(treeId, nodeId)
        val rightChild = forest.getRightChild(treeId, nodeId)
        forest.setSplit(treeId, rightChild, cut)

        val rightleftChild = forest.getLeftChild(treeId, rightChild)
        val rightrightChild = forest.getRightChild(treeId, rightChild)

        if(cut.labelNotValid.isDefined) {
          forest.setLabel(treeId, leftChild, cut.labelNotValid.get)
        }
        if (cut.labelNotValidOk == cut.notValid || forest.getLevel(treeId, leftChild) + 1 > property.value.maxDepth) {
          forest.setLeaf(treeId, leftChild)
        } else {
          if (featureSQRT.isDefined) {
            featureSQRT.get((treeId, leftChild)) = strategy.value.getStrategyFeature().getFeaturePerNode
          }
          mean.updateMean(cut.left)
        }

        if (cut.labelLeftOk == cut.left || forest.getLevel(treeId, rightleftChild) + 1 > property.value.maxDepth) {
          forest.setLeaf(treeId, rightleftChild)
          forest.setLabel(treeId, rightleftChild, cut.labelLeft.get)
        } else {
          if (featureSQRT.isDefined) {
            featureSQRT.get((treeId, rightleftChild)) = strategy.value.getStrategyFeature().getFeaturePerNode
          }
          mean.updateMean(cut.right)
        }

        if (cut.labelRightOk == cut.right || forest.getLevel(treeId, rightrightChild) + 1 > property.value.maxDepth) {
          forest.setLeaf(treeId, rightrightChild)
          forest.setLabel(treeId, rightrightChild, cut.labelRight.get)
        } else {
          if (featureSQRT.isDefined) {
            featureSQRT.get((treeId, rightrightChild)) = strategy.value.getStrategyFeature().getFeaturePerNode
          }
        }
      } else {
        forest.setSplit(treeId, nodeId, cutNotValid)
        if(cut.label.isDefined) forest.setLabel(treeId, nodeId, cut.label.get)

        val leftChild = forest.getLeftChild(treeId, nodeId)
        val rightChild = forest.getRightChild(treeId, nodeId)
        if (cut.labelLeftOk > cut.labelRightOk) {
          forest.setLabel(treeId, rightChild, cut.labelLeft.get)
        }
        else {
          forest.setLabel(treeId, rightChild, cut.labelRight.get)
        }
        forest.setLeaf(treeId, rightChild)

        if(cut.labelNotValid.isDefined) {
          forest.setLabel(treeId, leftChild, cut.labelNotValid.get)
        }
        if (cut.labelNotValidOk == cut.notValid || forest.getLevel(treeId, leftChild) + 1 > property.value.maxDepth) {
          forest.setLeaf(treeId, leftChild)
        } else {
          if (featureSQRT.isDefined) {
            featureSQRT.get((treeId, leftChild)) = strategy.value.getStrategyFeature().getFeaturePerNode
          }
          mean.updateMean(cut.left)
        }
      }
    } else {
      if (cut.left > 0 && cut.right > 0) {
        forest.setSplit(treeId, nodeId, cut)
        if(cut.label.isDefined) forest.setLabel(treeId, nodeId, cut.label.get)

        val leftChild = forest.getLeftChild(treeId, nodeId)
        val rightChild = forest.getRightChild(treeId, nodeId)

//        println(treeId+" "+nodeId+" "+cut.left+" "+cut.right+" "+leftChild+" "+rightChild)

        if (cut.labelLeft.isDefined) {
          forest.setLabel(treeId, leftChild, cut.labelLeft.get)
        }
        if (cut.labelRight.isDefined) {
          forest.setLabel(treeId, rightChild, cut.labelRight.get)
        }

        if (cut.labelLeftOk == cut.left || forest.getLevel(treeId, leftChild) + 1 > property.value.maxDepth) {
          forest.setLeaf(treeId, leftChild)
        } else {
          if (featureSQRT.isDefined) {
            featureSQRT.get((treeId, leftChild)) = strategy.value.getStrategyFeature().getFeaturePerNode
          }
          mean.updateMean(cut.left)
        }
        if (cut.labelRightOk == cut.right || forest.getLevel(treeId, rightChild) + 1 > property.value.maxDepth) {
          forest.setLeaf(treeId, rightChild)
        } else {
          if (featureSQRT.isDefined) {
            featureSQRT.get((treeId, rightChild)) = strategy.value.getStrategyFeature().getFeaturePerNode
          }
          mean.updateMean(cut.right)
        }
      } else {
        forest.setLabel(treeId, nodeId, cut.label.get)
        forest.setLeaf(treeId, nodeId)
      }
    }

    mean.getMean
  }
}

