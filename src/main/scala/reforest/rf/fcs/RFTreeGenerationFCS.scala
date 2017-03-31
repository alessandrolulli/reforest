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

package randomForest.test.reforest.rf

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import reforest.TypeInfo
import reforest.data.{DataOnWorker, StaticData, StaticDataClassic}
import reforest.dataTree.{Cut, TreeNode}
import reforest.rf.{RFEntropy, RFStrategy, RFTreeGeneration}
import reforest.rf.split.{RFSplitter, RFSplitterManager}
import reforest.util.{BiMap, CCUtil, GCInstrumented, MemoryUtil}

import scala.collection.{Map, mutable}
import scala.collection.mutable.ArrayBuffer

/* !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
This is under work. This functionality will be introduced in future versions and should not be used.
 */
class RFTreeGenerationFCS[T, U](@transient sc: SparkContext, binNumber: Int, maxNodesConcurrent: Int, typeInfo: Broadcast[TypeInfo[T]], typeInfoWorking: Broadcast[TypeInfo[U]], strategy: Broadcast[RFStrategy[T, U]], featureNumber: Int, tree: RFTreeGeneration[T, U]) extends Serializable {

  val entropy = sc.broadcast(new RFEntropy[T, U](typeInfo, typeInfoWorking))

  def finishSubTree(it: ((Int, Int), Iterable[StaticData[U]]),
                    featureMap: Broadcast[Map[(Int, Int), Array[Int]]],
                    rootArray: Broadcast[Array[TreeNode[T, U]]],
                    depthArgs: Int,
                    maxDepth: Int,
                    splitter: Broadcast[RFSplitterManager[T, U]],
                    numClasses: Int) = {
    var featureSQRT: collection.mutable.Map[(Int, Int), Array[Int]] = collection.mutable.Map(featureMap.value.map(t => (t._1, t._2)).toSeq: _*)

    var subTreeRoot: Option[TreeNode[T, U]] = Option.empty
    var treeId: Option[Int] = Option.empty

    var depth = depthArgs
    while (depth < maxDepth) {
      depth += 1

      if (subTreeRoot.isEmpty) {
        treeId = Some(it._1._1)
        subTreeRoot = Some(TreeNode.getNode(it._1._2, rootArray.value(treeId.get)))
      }

      var array: ArrayBuffer[Array[Int]] = ArrayBuffer.empty
      var idMap: collection.mutable.Map[(Int, Int), Int] = collection.mutable.Map[(Int, Int), Int]()

      it._2.foreach(w => {
        val workingData = w.getWorkingData(treeId.get)
        val nodeIdOption = subTreeRoot.get.getCurrentNodeId(workingData, typeInfoWorking.value)
        if (nodeIdOption.isDefined) {
          val nodeId = nodeIdOption.get
          var idx = idMap.get((treeId.get, nodeId))
          var row: Array[Int] = Array.empty

          val validFeatures = featureSQRT.get((treeId.get, nodeId)).getOrElse(Array.empty)

          if (idx.isEmpty) {
            idx = Some(array.length)
            idMap((treeId.get, nodeId)) = idx.get
            row = new Array(validFeatures.length * numClasses * (binNumber + 1)) //!!!!
            array += row
          } else {
            row = array(idx.get)
          }

          var featurePosition = 0
          while (featurePosition < validFeatures.length) {
            row(DataOnWorker.getColumn(featurePosition, binNumber, w.getLabel, typeInfoWorking.value.toInt(workingData.apply(validFeatures(featurePosition))), numClasses)) += w.getBagging(treeId.get)
            featurePosition += 1
          }
        }
      })
      //TODO unisci per evitare avere matrice, calcola subito best cut
      val featureSQRTold = featureSQRT
      featureSQRT = collection.mutable.Map()

      val idToId = new BiMap(idMap)
      var idx = 0

      while (idx < array.length) {
        //TODO usa la function invece di riscriverla qua
        //        val ((treeId, nodeId), bestSplit) = findBestCutEntropy(idx, array(idx), idToId, featureSQRTold, splitter.value, numClasses, depth, maxDepth)

        var bestSplit: Option[Cut[T, U]] = Option.empty
        val (treeId, nodeId) = idToId(idx)
        val validFeatures = featureSQRTold.get((treeId, nodeId)).getOrElse(Array.empty)

        var featurePosition = 0
        while (featurePosition < validFeatures.length) {
          val featureId = validFeatures(featurePosition)
          val offset = (featurePosition * numClasses * (binNumber + 1), (featurePosition + 1) * numClasses * (binNumber + 1))
          val rowSubset = tree.rowSubsetPrepare(array(idx), offset._1, offset._2, numClasses)
          val g = entropy.value.getBestSplit(rowSubset, featureId, splitter.value.getSplitter(0, treeId), depth, maxDepth, numClasses)
          if (bestSplit.isEmpty || g.stats > bestSplit.get.stats) bestSplit = Some(g)
          featurePosition += 1
        }

        if (bestSplit.isDefined) {
          val nodeToUpdate = TreeNode.getNode(nodeId, rootArray.value(treeId))
          tree.updateForest(bestSplit.get, treeId, nodeToUpdate, featureSQRT)
        }
        idx += 1
      }
    }

    (treeId.get, subTreeRoot.get)
  }

  def finishSubTree2(it: ((Int, Int), Iterable[StaticData[U]]),
                     featureMap: Broadcast[Map[(Int, Int), Array[Int]]],
                     rootArray: Broadcast[Array[TreeNode[T, U]]],
                     depthArgs: Int,
                     maxDepth: Int,
                     splitter: Broadcast[RFSplitterManager[T, U]],
                     numClasses: Int) = {
    var featureSQRT: collection.mutable.Map[(Int, Int), Array[Int]] = collection.mutable.Map(featureMap.value.map(t => (t._1, t._2)).toSeq: _*)

    val treeId: Int = it._1._1
    val subTreeRoot = TreeNode.getNode(it._1._2, rootArray.value(treeId))

    val nodeStack: mutable.Stack[(Int, Int)] = new mutable.Stack()
    nodeStack.push(it._1)

    while (!nodeStack.isEmpty) {
      val currentNode = nodeStack.pop()
      val validFeatures = featureSQRT.get((treeId, currentNode._2)).getOrElse(Array.empty)

      if (!validFeatures.isEmpty) {
        val array: Array[Int] = new Array(validFeatures.length * numClasses * (binNumber + 1)) //!!!!

        it._2.foreach(w => {
          val workingData = w.getWorkingData(treeId)
          val nodeIdOption = subTreeRoot.getCurrentNodeId(workingData, typeInfoWorking.value)
          if (nodeIdOption.isDefined) {
            val nodeId = nodeIdOption.get

            if (nodeId == currentNode._2) {
              var featurePosition = 0
              while (featurePosition < validFeatures.length) {
                array(DataOnWorker.getColumn(featurePosition, binNumber, w.getLabel, typeInfoWorking.value.toInt(workingData.apply(validFeatures(featurePosition))), numClasses)) += w.getBagging(treeId)
                featurePosition += 1
              }
            }
          }
        })

        featureSQRT.remove(currentNode)

        var bestSplit: Option[Cut[T, U]] = Option.empty

        var featurePosition = 0
        while (featurePosition < validFeatures.length) {
          val featureId = validFeatures(featurePosition)
          val offset = (featurePosition * numClasses * (binNumber + 1), (featurePosition + 1) * numClasses * (binNumber + 1))
          val rowSubset = tree.rowSubsetPrepare(array, offset._1, offset._2, numClasses)
          val g = entropy.value.getBestSplit(rowSubset, featureId, splitter.value.getSplitter(0, treeId), TreeNode.indexToLevel(currentNode._2) + 1, maxDepth, numClasses)
          if (bestSplit.isEmpty || g.stats > bestSplit.get.stats) bestSplit = Some(g)
          featurePosition += 1
        }

        if (bestSplit.isDefined) {
          val nodeToUpdate = TreeNode.getNode(currentNode._2, rootArray.value(treeId))
          tree.updateForest(bestSplit.get, treeId, nodeToUpdate, featureSQRT)

          if (nodeToUpdate.leftChild.isDefined && !nodeToUpdate.leftChild.get.isLeaf) nodeStack.push((treeId, nodeToUpdate.leftChild.get.id))
          if (nodeToUpdate.rightChild.isDefined && !nodeToUpdate.rightChild.get.isLeaf) nodeStack.push((treeId, nodeToUpdate.rightChild.get.id))
        } else {
          println("help")
        }
//        println(nodeStack.size)
      } else {
//        if(array.sum == 0)
        {
          println("MEGA HELP")

          it._2.foreach(w => {
            val workingData = w.getWorkingData(treeId)
            val nodeIdOption = subTreeRoot.getCurrentNodeId(workingData, typeInfoWorking.value)
            if (nodeIdOption.isDefined) {
              val nodeId = nodeIdOption.get

              if (nodeId == currentNode._2) {
                var featurePosition = 0
                while (featurePosition < validFeatures.length) {
//                  array(DataOnWorker.getColumn(featurePosition, binNumber, w.getLabel, typeInfoWorking.value.toInt(workingData.apply(validFeatures(featurePosition))), numClasses)) += w.getBagging(treeId)
                  featurePosition += 1
                }
              }
            }
          })
        }
      }
    }

    (treeId, subTreeRoot)
  }

  def sendWorkingData(it: Iterator[StaticData[U]],
                      featureMap: Broadcast[Map[(Int, Int), Array[Int]]],
                      rootArray: Broadcast[Array[TreeNode[T, U]]],
                      idTOid: Broadcast[BiMap[(Int, Int), Int]],
                      numClasses: Int,
                      nodeIdStart: Double,
                      nodeIdEnd: Double
                     ): Iterator[((Int, Int), StaticData[U])] = {
    it.flatMap(data => {
      val nodeIds = data.getBagging()
      nodeIds.zipWithIndex.flatMap {
        case (weight, treeId) =>
          val weight = nodeIds(treeId)
          if (weight > 0) {
            val point = data.getWorkingData(treeId)
            val nodeIdOption = rootArray.value(treeId).getCurrentNodeId(point, typeInfoWorking.value)
            if (nodeIdOption.isDefined) {
              val nodeId = nodeIdOption.get
              val controller = treeId + (nodeId.toDouble % 100) / 100
              if (controller >= nodeIdStart && controller < nodeIdEnd) {
                Iterator(((treeId, nodeId), new StaticDataClassic(data.getLabel, point, nodeIds)))
              } else {
                Iterator()
              }
            } else {
              Iterator()
            }
          } else {
            Iterator()
          }
      }
    })
  }

  def findBestCutFCS(sc: SparkContext,
                     dataIndex: RDD[StaticData[U]],
                     util: CCUtil,
                     splitter: Broadcast[RFSplitterManager[T, U]],
                     featureSelected: Map[(Int, Int), Array[Int]],
                     rootArrayArg: Array[TreeNode[T, U]],
                     depth: Int,
                     maxDepth: Int,
                     numClasses: Int,
                     appName: String,
                     instrumented: Broadcast[GCInstrumented],
                     memoryUtil: MemoryUtil): Array[TreeNode[T, U]] = {

    println("STARTING CYCLE " + depth)
    if (featureSelected.isEmpty) {
      Array[TreeNode[T, U]]()
    } else {
      val rootArray = sc.broadcast(rootArrayArg)

      val fMapTmp = featureSelected.map(t => (t._1, t._2.sorted))
      val featureIdMap = sc.broadcast(fMapTmp)
      val idTOid = sc.broadcast(new BiMap(featureSelected.toArray.map(t => t._1).sortBy(_._1).zipWithIndex.toMap))

      var toReturn = rootArray.value

      val maxTreeToCompute = memoryUtil.maxFCSPart

      var startIdNode = 0d
      var endIdNode = maxTreeToCompute
      var count = 0

      while (startIdNode < rootArrayArg.length) {
        count += 1
        println("STARTING FCS (" + maxTreeToCompute + ") " + count)

        //        val dataset = dataIndex.collect()

        val produce = dataIndex.mapPartitions(t => sendWorkingData(t, featureIdMap, rootArray, idTOid, numClasses, startIdNode, endIdNode))
        val aggregate = produce.groupByKey()
        val nodeToBeUpdated = aggregate.map(t => finishSubTree(t, featureIdMap, rootArray, depth, maxDepth, splitter, numClasses))

        val a = nodeToBeUpdated.collect()
        var c = 0
        while (c < a.length) {
          val parentIndex = TreeNode.parentIndex(a(c)._2.id)
          if (parentIndex != 0) {
            val parent = TreeNode.getNode(parentIndex, toReturn(a(c)._1))
            if (TreeNode.isLeftChild(a(c)._2.id)) {
              parent.leftChild = Some(a(c)._2)
            } else {
              parent.rightChild = Some(a(c)._2)
            }
          } else {
            toReturn(a(c)._1) = a(c)._2
          }
          c += 1
        }

        startIdNode = endIdNode
        endIdNode += maxTreeToCompute
        println("ENDING FCS " + count)
      }

      featureIdMap.unpersist()
      idTOid.unpersist()

      toReturn
    }
  }

  def getSQRTFeatures(): Array[Int] = {
    strategy.value.getSQRTFeatures()
  }
}