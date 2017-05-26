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

package reforest.rf.fcs

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import reforest.TypeInfo
import reforest.data.{DataOnWorker, StaticData}
import reforest.dataTree.{CutDetailed, Forest, ForestIncremental}
import reforest.rf.split.RFSplitterManager
import reforest.rf.{RFEntropy, RFProperty, RFStrategy, RFTreeGeneration}
import reforest.util.{Clear, MemoryUtil}

import scala.collection.mutable.ListBuffer
import scala.collection.{Map, mutable}

abstract class FCSExecutor[T, U](@transient private val sc: SparkContext,
                                 typeInfo: Broadcast[TypeInfo[T]],
                                 typeInfoWorking: Broadcast[TypeInfo[U]],
                                 strategy: Broadcast[RFStrategy[T, U]],
                                 property: Broadcast[RFProperty],
                                 tree: RFTreeGeneration[T, U],
                                 sampleSize: Long) extends Serializable {

  val entropy = sc.broadcast(new RFEntropy[T, U](typeInfo, typeInfoWorking))

  def init(depth: Int): Int

  def executeIteration(forest: Forest[T, U], dataIndex: RDD[StaticData[U]], depth: Int, iteration: Int, iterationNumber: Int): Forest[T, U]

  def finishSubTree(it: (Int, Iterable[StaticData[U]]),
                    stoppingCriteria: Option[Int],
                    staticData: Option[Broadcast[Array[StaticData[U]]]],
                    bucketMap: Broadcast[collection.immutable.Map[Int, Array[(Int, Int)]]],
                    forest: Broadcast[Forest[T, U]],
                    depthArgs: Int,
                    maxDepth: Int,
                    splitter: Broadcast[RFSplitterManager[T, U]],
                    numClasses: Int,
                    nodeStack: NodeProcessingList[(Int, Int, Option[ElementMapping])],
                    performInitialization: Boolean) = {
    val bucketId = it._1
    val forestToReturn = new ForestIncremental[T, U](property.value.numTrees, maxDepth)

    val elementArray: Array[StaticData[U]] = if (staticData.isDefined) {
      staticData.get.value
    } else {
      it._2.toArray
    }

    if (bucketMap.value.contains(bucketId)) {
      // INITIALIZING ELEMENT MAPPING FOR EFFICIENCY
      if (performInitialization && depthArgs != 0) {
        val elementMapping: mutable.Map[(Int, Int), ElementMapping] = mutable.Map.empty
        var elementArrayIndex = 0
        while (elementArrayIndex < elementArray.length) {
          val element = elementArray(elementArrayIndex)
          bucketMap.value(bucketId).foreach(nodeCurrent => {
            val treeId = nodeCurrent._1
            val nodeId = nodeCurrent._2
            val weight = element.getBagging(treeId)
            if (weight > 0) {
              val workingData = element.getWorkingData(treeId)
              val nodeIdCheckOption = forest.value.getCurrentNodeId(treeId, workingData, typeInfoWorking.value)
              if (nodeIdCheckOption.isDefined && nodeIdCheckOption.get == nodeId) {
                val mapping = elementMapping.get(nodeCurrent)
                if (mapping.isDefined) {
                  mapping.get.add(elementArrayIndex)
                } else {
                  val mappingToAdd = ElementMapping.build(sampleSize, depthArgs, maxDepth)
                  mappingToAdd.add(elementArrayIndex)
                  elementMapping.put(nodeCurrent, mappingToAdd)
                }
              }
            }
          })
          elementArrayIndex += 1
        }

        elementMapping.foreach(nodeAndMapping => nodeStack.add((nodeAndMapping._1._1, nodeAndMapping._1._2, Some(nodeAndMapping._2))))
        val activeTree: Array[Int] = elementMapping.map(t => t._1._1).toSet.toArray
        var count = 0
        while (count < activeTree.length) {
          forestToReturn.add(activeTree(count), forest.value.getTree(activeTree(count)))
          count += 1
        }
      } else {
        bucketMap.value(bucketId).foreach(node => nodeStack.add(node._1, node._2, Option.empty))
        val activeTree: Array[Int] = bucketMap.value(bucketId).map(t => t._1).toSet.toArray
        var count = 0
        while (count < activeTree.length) {
          forestToReturn.add(activeTree(count), forest.value.getTree(activeTree(count)))
          count += 1
        }
      }
      ///////////////////////////////////////////////////
      var nodesExecuted = 0
      def continueExecution = if (stoppingCriteria.isDefined && stoppingCriteria.get < nodesExecuted) {
        false
      } else {
        true
      }

      // TRYING TO HELP GC DOING LESS WORK REUSING THE FOLLOWING VARIABLES
      val array: Array[Int] = new Array(strategy.value.getStrategyFeature().getFeaturePerNodeNumber * numClasses * (property.value.binNumber + 1))
      val rowSubset = Array.tabulate((property.value.binNumber + 1))(_ => new Array[Int](numClasses))
      //////////////////////////////////////////////////////////////////////////

      while (!nodeStack.isEmpty && continueExecution) {
        nodesExecuted += 1
        Clear.clear(array)

        val currentNodeAndMapping = nodeStack.get()
        val treeId: Int = currentNodeAndMapping._1
        val nodeId: Int = currentNodeAndMapping._2
        val mapping = currentNodeAndMapping._3
        val currentDepth = forest.value.getLevel(treeId, nodeId)
        val validFeatures = strategy.value.getStrategyFeature().getFeaturePerNode

        val mappingForChilds = ElementMapping.build(sampleSize, currentDepth, maxDepth)
        if (mapping.isDefined) {
          val parentNode = forest.value.getParent(treeId, nodeId)
          mapping.get.getMapping.foreach(elementArrayIndex => {
            val element = elementArray(elementArrayIndex)
            val workingData = element.getWorkingData(treeId)
            val nodeIdCheckOption = forest.value.getCurrentNodeId(treeId, parentNode, workingData, typeInfoWorking.value)
            if (nodeIdCheckOption.isDefined && nodeIdCheckOption.get == nodeId) {
              mappingForChilds.add(elementArrayIndex)
              var featurePosition = 0
              while (featurePosition < validFeatures.length) {
                array(DataOnWorker.getColumn(featurePosition, property.value.binNumber, element.getLabel, typeInfoWorking.value.toInt(workingData.apply(validFeatures(featurePosition))), numClasses)) += element.getBagging(treeId)
                featurePosition += 1
              }
            }
          })
        } else {
          var elementArrayIndex = 0
          while (elementArrayIndex < elementArray.length) {
            val element = elementArray(elementArrayIndex)
            val weight = element.getBagging(treeId)
            if (weight > 0) {
              val workingData = element.getWorkingData(treeId)
              val nodeIdCheckOption = forest.value.getCurrentNodeId(treeId, workingData, typeInfoWorking.value)
              if (nodeIdCheckOption.isDefined && nodeIdCheckOption.get == nodeId) {
                mappingForChilds.add(elementArrayIndex)
                var featurePosition = 0
                while (featurePosition < validFeatures.length) {
                  array(DataOnWorker.getColumn(featurePosition, property.value.binNumber, element.getLabel, typeInfoWorking.value.toInt(workingData.apply(validFeatures(featurePosition))), numClasses)) += weight
                  featurePosition += 1
                }
              }
            }
            elementArrayIndex += 1
          }
        }
        var bestSplit: Option[CutDetailed[T, U]] = Option.empty
        val splitterForNode = splitter.value.getSplitter(0, treeId)
        var featurePosition = 0
        while (featurePosition < validFeatures.length) {
          val featureId = validFeatures(featurePosition)
          val offset = (featurePosition * numClasses * (property.value.binNumber + 1), (featurePosition + 1) * numClasses * (property.value.binNumber + 1))
          tree.rowSubsetPrepareWithData(rowSubset, array, offset._1, offset._2, numClasses)
          val g = entropy.value.getBestSplit(rowSubset, featureId, splitterForNode, currentDepth, maxDepth, numClasses)
          if (bestSplit.isEmpty || g.stats > bestSplit.get.stats) bestSplit = Some(g)
          featurePosition += 1
        }
        if (bestSplit.isDefined) {
          tree.updateForest(bestSplit.get, treeId, nodeId, forest.value)
          if (forestToReturn.isDefined(treeId, forestToReturn.getLeftChild(treeId, nodeId)) && !forestToReturn.isLeaf(treeId, forestToReturn.getLeftChild(treeId, nodeId))) {
            nodeStack.add((treeId, forestToReturn.getLeftChild(treeId, nodeId), Some(mappingForChilds)))
          }
          if (forest.value.isDefined(treeId, forest.value.getRightChild(treeId, nodeId)) && !forest.value.isLeaf(treeId, forest.value.getRightChild(treeId, nodeId))) {
            nodeStack.add((treeId, forestToReturn.getRightChild(treeId, nodeId), Some(mappingForChilds)))
          }
        }
      }
    }

    Iterator(forestToReturn)
  }
}

object FCSExecutor {
  def build[T, U](@transient sc: SparkContext,
                  typeInfo: Broadcast[TypeInfo[T]],
                  typeInfoWorking: Broadcast[TypeInfo[U]],
                  splitter: Broadcast[RFSplitterManager[T, U]],
                  property: Broadcast[RFProperty],
                  strategy: Broadcast[RFStrategy[T, U]],
                  memoryUtil: MemoryUtil,
                  featureIdMap: Broadcast[Map[(Int, Int), Array[Int]]],
                  tree: RFTreeGeneration[T, U],
                  sampleSize: Long,
                  depth: Int) = {
    property.value.fcsCycleActivation = depth
    if (depth == 0) {
      println("FCS ALL")
      new FCSExecutorAll[T, U](sc, typeInfo, typeInfoWorking, splitter, property, strategy, memoryUtil, featureIdMap,
        tree,
        sampleSize)
    } else {
      println("FCS BUCKET")
      new FCSExecutorBucketBC[T, U](sc, typeInfo, typeInfoWorking, splitter, property, strategy, memoryUtil, featureIdMap,
        tree,
        sampleSize)
    }
  }
}

class FCSExecutorAll[T, U](@transient private val sc: SparkContext,
                           typeInfo: Broadcast[TypeInfo[T]],
                           typeInfoWorking: Broadcast[TypeInfo[U]],
                           splitter: Broadcast[RFSplitterManager[T, U]],
                           property: Broadcast[RFProperty],
                           strategy: Broadcast[RFStrategy[T, U]],
                           memoryUtil: MemoryUtil,
                           featureIdMap: Broadcast[Map[(Int, Int), Array[Int]]],
                           tree: RFTreeGeneration[T, U],
                           sampleSize: Long) extends FCSExecutor[T, U](sc, typeInfo, typeInfoWorking, strategy, property,
  tree,
  sampleSize) {
  def init(depth: Int) = {
    1
  }

  def execute(nodes: Iterable[Array[(Int, Int)]],
              limit: Option[Int],
              clusterNumber: Int,
              forest: Forest[T, U],
              dataIndexBC: Option[Broadcast[Array[StaticData[U]]]],
              depth: Int,
              nodeProcessingList: NodeProcessingList[(Int, Int, Option[ElementMapping])]): Forest[T, U] = {
    val bucketBC = sc.broadcast(nodes.toArray.zipWithIndex.map(_.swap).toMap)
    val forestBC = sc.broadcast(forest)

    val forestUpdated = sc.parallelize(bucketBC.value.keys.map(i => (i, Iterable.empty)).toSeq, clusterNumber)
      .flatMap(bucketId => {
        finishSubTree(bucketId, limit, dataIndexBC, bucketBC, forestBC, depth, property.value.maxDepth, splitter, property.value.numClasses, nodeProcessingList, true)
      }).reduce((a, b) => {
      a.merge(b); a
    })

    bucketBC.destroy()
    forestBC.destroy()

    forestUpdated
  }

  def executeIteration(forest: Forest[T, U], dataIndex: RDD[StaticData[U]], depth: Int,
                       iteration: Int, iterationNumber: Int): Forest[T, U] = {
    // DATASET
    val t0 = System.currentTimeMillis()
    val dataIndexBC: Option[Broadcast[Array[StaticData[U]]]] = Some(sc.broadcast(dataIndex.collect()))
    dataIndex.unpersist()
    val t01 = System.currentTimeMillis()

    // CLUSTER STATS
    val minClusterNumber = property.value.sparkCoresMax * 4
    var clusterNumber = property.value.numTrees // Math.min(minClusterNumber, numTrees)
    var clusterSize = Math.ceil(property.value.numTrees.toDouble / clusterNumber).toInt
    var bucket = featureIdMap.value.map(t => t._1).toList.sortBy(_._1).toArray.zipWithIndex.map(t => (t._2 / clusterSize, t._1)).groupBy(_._1).map(t => t._2.map(u => u._2))
    var forestToUse = forest

    // PRE-PROCESSING IF TOO LESS CLUSTERS
    if (clusterNumber < minClusterNumber) {
      val requestedCyclePerCluster = Math.ceil(minClusterNumber.toDouble / property.value.numTrees).toInt - 1
      println("FCS PRE-PROCESSING CLUSTER NUMBER: " + clusterNumber + " SIZE: " + clusterSize + " " + requestedCyclePerCluster)
      forestToUse = execute(bucket, Some(requestedCyclePerCluster), clusterNumber, forestToUse, dataIndexBC, depth, NodeProcessingQueue.create)
      val toUpdate: ListBuffer[(Int, Int)] = forestToUse.getNodeToBeComputed()
      clusterNumber = toUpdate.size
      clusterSize = Math.ceil(toUpdate.size.toDouble / clusterNumber).toInt
      bucket = toUpdate.toArray.zipWithIndex.map(t => (t._2 / clusterSize, t._1)).groupBy(_._1).map(t => t._2.map(u => u._2))
    }

    // EXECUTION
    println("FCS CLUSTER NUMBER: " + clusterNumber + " SIZE: " + clusterSize)
    val forestUpdated = execute(bucket, Option.empty, clusterNumber, forestToUse, dataIndexBC, depth, NodeProcessingStack.create)

    dataIndexBC.get.destroy()
    val t1 = System.currentTimeMillis()
    println("ONLY FCS TIME: " + (t1 - t0) + " " + (t01 - t0))
    forestUpdated
  }
}

class FCSExecutorBucketBC[T, U](@transient private val sc: SparkContext,
                                typeInfo: Broadcast[TypeInfo[T]],
                                typeInfoWorking: Broadcast[TypeInfo[U]],
                                splitter: Broadcast[RFSplitterManager[T, U]],
                                property: Broadcast[RFProperty],
                                strategy: Broadcast[RFStrategy[T, U]],
                                memoryUtil: MemoryUtil,
                                featureIdMap: Broadcast[Map[(Int, Int), Array[Int]]],
                                tree: RFTreeGeneration[T, U],
                                sampleSize: Long) extends FCSExecutor[T, U](sc, typeInfo, typeInfoWorking, strategy, property,
  tree: RFTreeGeneration[T, U],
  sampleSize) {

  val nodeNumber = featureIdMap.value.size
  val clusterNumber = memoryUtil.fcsClusterNumber(nodeNumber)
  val clusterSize = memoryUtil.fcsClusterSize()

  val nodes = featureIdMap.value.toArray.map(t => t._1)
  val bucket = nodes.zipWithIndex.map(t => (t._2 / clusterSize, t._1)).groupBy(_._1).map(t => t._2.map(u => u._2)).toArray
  featureIdMap.destroy()

  override def init(depth: Int) = {
    val maxBucketConcurrent = memoryUtil.fcsMaximumClusterConcurrent(clusterNumber, depth)
    println("MAX CONCURRENT CLUSTERS: " + maxBucketConcurrent + "/" + clusterNumber + " CLUSTER SIZE: " + clusterSize)
    Math.ceil(clusterNumber.toDouble / maxBucketConcurrent).toInt
  }

  def execute(nodes: Iterable[Array[(Int, Int)]],
              limit: Option[Int],
              clusterNumber: Int,
              forestBC: Broadcast[Forest[T, U]],
              dataIndexBC: Option[Broadcast[Array[StaticData[U]]]],
              depth: Int,
              nodeProcessingList: NodeProcessingList[(Int, Int, Option[ElementMapping])]) = {
    val bucketBC = sc.broadcast(nodes.toArray.zipWithIndex.map(_.swap).toMap)

    val forestUpdated = sc.parallelize(bucketBC.value.keys.map(i => (i, Iterable.empty)).toSeq, clusterNumber)
      .flatMap(bucketId => {
        finishSubTree(bucketId, limit, dataIndexBC, bucketBC, forestBC, depth, property.value.maxDepth, splitter, property.value.numClasses, nodeProcessingList, false)
      }).reduce((a, b) => {
      a.merge(b); a
    })

    bucketBC.destroy()

    forestUpdated
  }

  override def executeIteration(forest: Forest[T, U],
                                dataIndex: RDD[StaticData[U]],
                                depth: Int,
                                iteration: Int,
                                iterationNumber: Int): Forest[T, U] = {
    val forestBC = sc.broadcast(forest)
    val maxBucketConcurrent = memoryUtil.fcsMaximumClusterConcurrent(clusterNumber, depth)

    val from = iteration * maxBucketConcurrent
    val to = from + maxBucketConcurrent
    val validBucket = bucket.slice(from, to)
    val validNodesBC = sc.broadcast(validBucket.flatMap(t => t.map(u => u)).toSet)
    val bucketForIteration = validNodesBC.value.toArray.map(t => Array(t))

    val dataIndexBC: Option[Broadcast[Array[StaticData[U]]]] =
      if (iterationNumber == 1) {
        val tmp = Some(sc.broadcast(dataIndex.collect()))
        dataIndex.unpersist()
        tmp
      } else {

        val tmp = Some(sc.broadcast(dataIndex.mapPartitions(t => filterWorkingDataBucket(t, validNodesBC, forestBC)).collect()))

        tmp
      }

    val forestUpdated = execute(bucketForIteration, Option.empty, bucketForIteration.length, forestBC, dataIndexBC, depth, NodeProcessingStack.create)

    forestBC.unpersist()
    dataIndexBC.get.destroy()
    validNodesBC.destroy()

    forest.merge(forestUpdated)
    forest
  }

  def filterWorkingDataBucket(it: Iterator[StaticData[U]],
                              validNodes: Broadcast[Set[(Int, Int)]],
                              forest: Broadcast[Forest[T, U]]
                             ): Iterator[StaticData[U]] = {
    val validTreeId = validNodes.value.map(t => t._1).toArray

    it.flatMap(data => {
      var selected = false
      var count = 0
      while (count < validTreeId.length && !selected) {
        val treeId = validTreeId(count)
        if (data.getBagging(treeId) > 0) {
          val nodeIdOption = forest.value.getCurrentNodeId(treeId, data.getWorkingData(treeId), typeInfoWorking.value)
          if (nodeIdOption.isDefined) {
            val nodeId = nodeIdOption.get
            if (validNodes.value.contains((treeId, nodeId))) {
              selected = true
            }
          }
        }
        count += 1
      }

      if (selected) {
        Iterator(data)
      } else {
        Iterator()
      }
    })
  }
}