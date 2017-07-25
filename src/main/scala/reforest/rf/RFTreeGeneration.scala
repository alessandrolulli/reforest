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

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import reforest.TypeInfo
import reforest.data.StaticData
import reforest.data.matrix.{DataOnWorker, DataOnWorkerManager}
import reforest.data.tree.{CutDetailed, Forest, ForestManager, NodeId}
import reforest.rf.feature.RFFeatureManager
import reforest.rf.parameter.RFParameter
import reforest.util.{BiMap, GCInstrumented, IncrementalMean}

import scala.reflect.ClassTag

class RFTreeGeneration[T: ClassTag, U: ClassTag](@transient private val sc: SparkContext,
                                                 property: Broadcast[RFParameter],
                                                 typeInfo: Broadcast[TypeInfo[T]],
                                                 typeInfoWorking: Broadcast[TypeInfo[U]],
                                                 categoricalFeatures: Broadcast[RFCategoryInfo],
                                                 strategy: Broadcast[RFStrategy[T, U]],
                                                 numTrees: Int,
                                                 macroIteration: Int) extends Serializable {

  val entropy = sc.broadcast(new RFEntropy[T, U](typeInfo, typeInfoWorking))

  def findBestCutEntropy(idx: Int,
                         matrixRow: Array[Int],
                         featureManager: RFFeatureManager,
                         forestManager: ForestManager[T, U],
                         depth: Int,
                         idToId: BiMap[NodeId, Int]): (NodeId, CutDetailed[T, U]) = {
    var bestSplit: Option[CutDetailed[T, U]] = Option.empty
    val matrixId = idToId(idx)
    val featurePerNode = featureManager.getFeature(matrixId).getOrElse(Array.empty[Int])

    var featurePosition = 0
    while (featurePosition < featurePerNode.length) {
      val featureId = featurePerNode(featurePosition)
      val offset = DataOnWorkerManager.getForestMatrix(matrixId.forestId).getOffset(idx, featurePosition)
      val rowSubset = RFTreeGeneration.rowSubsetPrepare(matrixRow, offset._1, offset._2, property.value.numClasses)
      val g = if (categoricalFeatures.value.isCategorical(featureId)) entropy.value.getBestSplitCategorical(rowSubset, featureId, forestManager.splitterManager.getSplitter(macroIteration, matrixId.treeId), depth, property.value.getMaxDepth, property.value.numClasses)
      else entropy.value.getBestSplit(rowSubset, featureId, forestManager.splitterManager.getSplitter(macroIteration, matrixId.treeId), forestManager.getForest(matrixId.forestId).featureSizer, depth, property.value.getMaxDepth, property.value.numClasses)
      if (bestSplit.isEmpty || g.stats > bestSplit.get.stats) bestSplit = Some(g)
      featurePosition += 1
    }

    (matrixId, bestSplit.get)
  }

  def mapPartitionsPointToMatrix(it: Iterator[StaticData[U]],
                                 featureManager: RFFeatureManager,
                                 iteration: Int,
                                 iterationNum: Int,
                                 forestManager: ForestManager[T, U],
                                 idToId: Array[BiMap[NodeId, Int]],
                                 skip: RFSkip): Unit = {
    it.foreach { data =>
      var forestId = 0
      while (forestId < forestManager.getForest.length) {
        var treeId = 0
        val forest = forestManager.getForest(forestId)
        if (!skip.contains(forest)) {
          val matrix = DataOnWorkerManager.getForestMatrix(forestId)
          while (treeId < numTrees) {
            val weight = data.getBagging(treeId)
            if (weight > 0 && (iterationNum <= 1 || treeId % iterationNum == iteration)) {
              val point = data.getWorkingData(treeId)
              val nodeIdOption = forest.getCurrentNodeId(treeId, point, typeInfoWorking.value)
              if (nodeIdOption.isDefined) {
                val nodeId = nodeIdOption.get
                val node = NodeId(forestId, treeId, nodeId)
                val idx = idToId(forestId)(node)
                val idArray = matrix.array(idx)
                val validFeatures: Array[Int] = featureManager.getFeature(node).getOrElse(Array.empty)
                var featurePosition = 0
                val featurePositionArray = matrix.getOffset(idx)
                idArray.synchronized({
                  while (featurePosition < validFeatures.length) {
                    idArray(DataOnWorker.getPositionOffset(featurePositionArray(featurePosition), data.getLabel, forest.featureSizer.getShrinkedValue(validFeatures(featurePosition), typeInfoWorking.value.toInt(point(validFeatures(featurePosition)))), property.value.numClasses)) += weight
                    featurePosition += 1
                  }
                })
              }
            }
            treeId += 1
          }
        }
        forestId += 1
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
                  forestManager: ForestManager[T, U],
                  featureManager: RFFeatureManager,
                  depth: Int,
                  instrumented: Broadcast[GCInstrumented],
                  maxNodesConcurrent: Int,
                  skip: RFSkip) = {

    val iterationNumber = Math.ceil(featureManager.getActiveNodesNum.toDouble / maxNodesConcurrent).toInt
    var iteration = 0

    val forestManagerBC = sc.broadcast(forestManager)
    val featureManagerBC = sc.broadcast(featureManager)
    val skipBC = sc.broadcast(skip)

    var toReturn: Array[(NodeId, CutDetailed[T, U])] = Array.empty

    instrumented.value.start()

    while (iteration < iterationNumber) {

      val idToIdBC = sc.broadcast(featureManager.generateIdToId(iteration, iterationNumber))

      dataIndex.foreachPartition(_ => {
        /* Matrix initialization */
        instrumented.value.gc(DataOnWorkerManager.init(forestManagerBC.value, featureManagerBC.value, depth, iteration, iterationNumber, property.value.numClasses, idToIdBC.value, skipBC.value))
      })

      dataIndex.foreachPartition(t => {
        /* Local information collection */
        instrumented.value.gc(mapPartitionsPointToMatrix(t, featureManagerBC.value, iteration, iterationNumber, forestManagerBC.value, idToIdBC.value, skipBC.value))
      })

      /* Distributed Information Aggregation + Best Cut Finding */
      toReturn = toReturn ++ dataIndex.mapPartitions { _ =>
        val matrixSet = DataOnWorkerManager.getMatrix
        if (matrixSet.isDefined) {
          val toReturn = matrixSet.get.flatMap { case (matrix, forestId) => matrix.array.zipWithIndex.map { case (row, idx) => ((forestId, idx), row) } }.iterator
          DataOnWorkerManager.releaseMatrix
          toReturn
        } else
          Iterator()
      }.reduceByKey(reduceMatrix)
        .mapPartitions(t => t.map { case ((forestId, idx), row) => findBestCutEntropy(idx, row, featureManagerBC.value, forestManagerBC.value, depth, idToIdBC.value(forestId)) })
        .collect()


      if (instrumented.value.valid)
        dataIndex.foreachPartition(_ => instrumented.value.gc())

      iteration += 1
      idToIdBC.unpersist()
    }

    instrumented.value.stop()

    forestManagerBC.unpersist()
    featureManagerBC.unpersist()
    skipBC.unpersist()

    toReturn
  }
}

object RFTreeGeneration {
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
}
