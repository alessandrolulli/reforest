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

package reforest.rf.slc

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import reforest.TypeInfo
import reforest.data.StaticData
import reforest.data.matrix.DataOnWorker
import reforest.data.tree._
import reforest.rf.feature.RFFeatureManager
import reforest.rf.parameter.RFParameter
import reforest.rf.split.RFSplitterManager
import reforest.rf.{RFEntropy, RFSkip, RFTreeGeneration}
import reforest.util.Clear

/**
  * The executor of the SLC computation.
  *
  * @param sc              the Spark context
  * @param typeInfo        the type information for the raw data
  * @param typeInfoWorking the type information for the working data
  * @param property        the configuration properties
  * @param sampleSize      the size of the dataset
  * @tparam T raw data type
  * @tparam U working data type
  */
abstract class SLCExecutor[T, U](@transient private val sc: SparkContext,
                                 typeInfo: Broadcast[TypeInfo[T]],
                                 typeInfoWorking: Broadcast[TypeInfo[U]],
                                 property: Broadcast[RFParameter],
                                 sampleSize: Long) extends Serializable {

  protected val entropy = sc.broadcast(new RFEntropy[T, U](typeInfo, typeInfoWorking))

  /**
    * Execute the SLC computation
    *
    * @param forestManager  the forest manager
    * @param featureManager the feature manager
    * @param dataIndex      the local dataset
    * @param depthToStop    the max depth to be computed in this method
    * @return the updated forest manager
    */
  def executeSLC(forestManager: ForestManager[T, U],
                 featureManager: RFFeatureManager,
                 dataIndex: RDD[StaticData[U]],
                 depthToStop: Int,
                 skip: RFSkip): ForestManager[T, U]

  /**
    * This must be rewritten better.
    *
    * It completes the computation of a sub-tree locally
    *
    * @param node              the starting node or the root of the active tree to be computed
    * @param staticData        the local dataset
    * @param forestManager     the forest manager
    * @param maxDepth          the max depth to be computed
    * @param maxDepthToCompute the max depth to be computed in this method
    * @return an updated forest
    */
  def finishSubTree(node: NodeId,
                    staticData: Broadcast[Array[StaticData[U]]],
                    forestManager: Broadcast[ForestManager[T, U]],
                    maxDepth: Int,
                    maxDepthToCompute: Int,
                    skipper: RFSkip): Forest[T, U] = {

    // if subsequent call needs to execute still the root must not execute
    /////////////////////////////////////////////////NA MERDA
    // a different logic must be implemented if the local stored has data or not !!!!!
    // this method must be called one time for each node stored in the SLCStorer
    val nodeStack: NodeProcessingList[NodeStackContainer] = SLCStorer.get(node).getOrElse(NodeProcessingStack.create)
    val nodeStackToStore: NodeProcessingList[NodeStackContainer] = NodeProcessingStack.create
    if (nodeStack.isEmpty) {
      nodeStack.add(NodeStackContainer(node.nodeId, Option.empty))
    }
    /////////////////////////////////////////////////////////7
    val forest = forestManager.value.getForest(node.forestId)

    if (skipper.contains(forest.binNumber, forest.featurePerNode.getFeaturePerNodeNumber)) {
      nodeStack.clear
    }

    val forestToReturn = new ForestIncremental[T, U](forest)
    forestToReturn.add(node.treeId, forestManager.value.getForest(node.forestId).getTree(node.treeId))

    val elementArray: Array[StaticData[U]] = staticData.value
    ///////////////////////////////////////////////////
    // TRYING TO HELP GC DOING LESS WORK REUSING THE FOLLOWING VARIABLES
    val array: Array[Int] = new Array(forest.featurePerNode.getFeaturePerNodeNumber *
      property.value.numClasses *
      (forest.binNumber + 1))
    val rowSubset = Array.tabulate((forest.binNumber + 1))(_ => new Array[Int](property.value.numClasses))
    //////////////////////////////////////////////////////////////////////////
    val splitterForNode = forestManager.value.splitterManager.getSplitter(0, node.treeId)

    while (!nodeStack.isEmpty) {
      Clear.clear(array)

      val currentNodeAndMapping = nodeStack.get()
      val nodeId: Int = currentNodeAndMapping.nodeId

      val mapping = currentNodeAndMapping.mapping
      val currentDepth = forest.getLevel(node.treeId, nodeId)

      val validFeatures = forest.featurePerNode.getFeaturePerNode

      val mappingForChilds = ElementMapping.build(sampleSize, currentDepth, maxDepth)
      if (mapping.isDefined) {
        val parentNode = forest.getParent(node.treeId, nodeId)
        mapping.get.getMapping.foreach(elementArrayIndex => {
          val element = elementArray(elementArrayIndex)
          val workingData = element.getWorkingData(node.treeId)
          val nodeIdCheckOption = forest.getCurrentNodeId(node.treeId, parentNode, workingData, typeInfoWorking.value)
          if (nodeIdCheckOption.isDefined && nodeIdCheckOption.get == nodeId) {
            mappingForChilds.add(elementArrayIndex)
            var featurePosition = 0
            while (featurePosition < validFeatures.length) {
              array(DataOnWorker.getColumn(featurePosition, forest.binNumber, element.getLabel,
                forest.featureSizer.getShrinkedValue(validFeatures(featurePosition),
                  typeInfoWorking.value.toInt(workingData.apply(validFeatures(featurePosition)))),
                property.value.numClasses)) += element.getBagging(node.treeId)
              featurePosition += 1
            }
          }
        })
      } else {
        var elementArrayIndex = 0
        while (elementArrayIndex < elementArray.length) {
          val element = elementArray(elementArrayIndex)
          val weight = element.getBagging(node.treeId)
          if (weight > 0) {
            val workingData = element.getWorkingData(node.treeId)
            val nodeIdCheckOption = forest.getCurrentNodeId(node.treeId, workingData, typeInfoWorking.value)
            if (nodeIdCheckOption.isDefined && nodeIdCheckOption.get == nodeId) {
              mappingForChilds.add(elementArrayIndex)
              var featurePosition = 0
              while (featurePosition < validFeatures.length) {
                array(DataOnWorker.getColumn(featurePosition, forest.binNumber, element.getLabel, forest.featureSizer.getShrinkedValue(validFeatures(featurePosition), typeInfoWorking.value.toInt(workingData.apply(validFeatures(featurePosition)))), property.value.numClasses)) += weight
                featurePosition += 1
              }
            }
          }
          elementArrayIndex += 1
        }
      }
      var bestSplit: Option[CutDetailed[T, U]] = Option.empty
      var featurePosition = 0
      while (featurePosition < validFeatures.length) {
        val featureId = validFeatures(featurePosition)
        val offset = (featurePosition * property.value.numClasses * (forest.binNumber + 1), (featurePosition + 1) * property.value.numClasses * (forest.binNumber + 1))
        RFTreeGeneration.rowSubsetPrepareWithData(rowSubset, array, offset._1, offset._2, property.value.numClasses)
        val g = entropy.value.getBestSplit(rowSubset, featureId, splitterForNode, forest.featureSizer, currentDepth, maxDepth, property.value.numClasses)
        if (bestSplit.isEmpty || g.stats > bestSplit.get.stats) bestSplit = Some(g)
        featurePosition += 1
      }
      if (bestSplit.isDefined) {
        forestToReturn.updateForest(bestSplit.get, node.forestId, node.treeId, nodeId, typeInfo.value, typeInfoWorking.value)

        if (!forestToReturn.isLeaf(node.treeId, nodeId)) {
          val leftChildId = forestToReturn.getLeftChild(node.treeId, nodeId)
          val rightChildId = forestToReturn.getRightChild(node.treeId, nodeId)

          if (forestToReturn.isDefined(node.treeId, leftChildId) && !forestToReturn.isLeaf(node.treeId, leftChildId)) {
            if (maxDepthToCompute != -1 && forestToReturn.getLevel(node.treeId, leftChildId) > maxDepthToCompute) {
              //              if (mappingForChilds.toStore.isDefined)
              nodeStackToStore.add(NodeStackContainer(leftChildId, mappingForChilds.toStore))
            } else {
              nodeStack.add(NodeStackContainer(leftChildId, mappingForChilds.toStore))
            }
          }
          if (forest.isDefined(node.treeId, rightChildId) && !forest.isLeaf(node.treeId, rightChildId)) {
            if (maxDepthToCompute != -1 && forestToReturn.getLevel(node.treeId, rightChildId) > maxDepthToCompute) {
              //              if (mappingForChilds.toStore.isDefined)
              nodeStackToStore.add(NodeStackContainer(rightChildId, mappingForChilds.toStore))
            } else {
              nodeStack.add(NodeStackContainer(rightChildId, mappingForChilds.toStore))
            }
          }
        }
      }
    }

    if (!nodeStackToStore.isEmpty) {
      SLCStorer.store(node, nodeStackToStore)
    }

    forestToReturn
  }
}

/**
  * An utility to build a SLCExecutor
  */
object SLCExecutor {
  /**
    * Build a SLExecutor
    *
    * @param sc              the Spark context
    * @param typeInfo        the type information for the raw data
    * @param typeInfoWorking the typeinformation for the working data
    * @param property        the configuration properties
    * @param splitter        the manager to split the raw data
    * @param sampleSize      the size of the dataset
    * @tparam T raw data type
    * @tparam U working data type
    * @return a built SLCExecutor
    */
  def build[T, U](@transient sc: SparkContext,
                  typeInfo: Broadcast[TypeInfo[T]],
                  typeInfoWorking: Broadcast[TypeInfo[U]],
                  property: Broadcast[RFParameter],
                  splitter: Broadcast[RFSplitterManager[T, U]],
                  sampleSize: Long) = {
    new SLCExecutorAllMergeForest[T, U](sc,
      typeInfo,
      typeInfoWorking,
      property,
      sampleSize)
  }
}

/**
  * The base implementation of the SLCExecutor
  *
  * @param sc              the Spark context
  * @param typeInfo        the type information for the raw data
  * @param typeInfoWorking the type information for the working data
  * @param property        the configuration properties
  * @param sampleSize      the size of the dataset
  * @tparam T raw data type
  * @tparam U working data type
  */
abstract class SLCExecutorAll[T, U](@transient private val sc: SparkContext,
                                    typeInfo: Broadcast[TypeInfo[T]],
                                    typeInfoWorking: Broadcast[TypeInfo[U]],
                                    property: Broadcast[RFParameter],
                                    sampleSize: Long) extends SLCExecutor[T, U](sc, typeInfo, typeInfoWorking, property, sampleSize) {

  private val slcDataset = new SLCDataset[U](sc)

  def execute(forestManager: ForestManager[T, U],
              depthToStop: Int,
              nodeProcessingList: NodeProcessingList[NodeStackContainer],
              skip: RFSkip): ForestManager[T, U] = {
    val forestManagerBC = sc.broadcast(forestManager)

    val bucket = slcDataset.getActiveRoot
    val localDataset = slcDataset.getLocalDataset
    val skipBC = sc.broadcast(skip)

    val forestUpdated = sc.parallelize(bucket, property.value.getSparkPartitionSLC(bucket.length)).mapPartitionsWithIndex { case (_, it) => it.map { node => {
      (node.forestId, finishSubTree(node, localDataset, forestManagerBC, property.value.getMaxDepth, depthToStop, skipBC.value))
    }
    }
    }.reduceByKey((a, b) => {
      a.merge(b);
      a
    })
      .map { case (forestId, forest) => forest }.collect()

    forestManagerBC.unpersist()
    skipBC.unpersist()

    new ForestManager[T, U](property.value, forestManager.splitterManager, forestUpdated)
  }

  override def executeSLC(forestManager: ForestManager[T, U],
                          featureManager: RFFeatureManager,
                          dataIndex: RDD[StaticData[U]],
                          depthToStop: Int,
                          skip: RFSkip): ForestManager[T, U] = {
    val t0 = System.currentTimeMillis()

    slcDataset.init(dataIndex, featureManager, forestManager.getNumTrees)

    if (slcDataset.getClear) dataIndex.foreachPartition(_ => SLCStorer.clear())

    val forestUpdated = execute(forestManager, depthToStop, NodeProcessingStack.create, skip)

    val t1 = System.currentTimeMillis()
    println("SLC TIME ALL: " + (t1 - t0))

    forestUpdated
  }
}

/**
  * A SLCExecutor able to merge the forests
  *
  * @param sc              the Spark context
  * @param typeInfo        the type information for the raw data
  * @param typeInfoWorking the type information for the working data
  * @param property        the configuration properties
  * @param sampleSize      the size of the dataset
  * @tparam T raw data type
  * @tparam U working data type
  */
class SLCExecutorAllMergeForest[T, U](@transient private val sc: SparkContext,
                                      typeInfo: Broadcast[TypeInfo[T]],
                                      typeInfoWorking: Broadcast[TypeInfo[U]],
                                      property: Broadcast[RFParameter],
                                      sampleSize: Long) extends SLCExecutorAll(sc, typeInfo, typeInfoWorking, property, sampleSize) {

  override def executeSLC(forestManager: ForestManager[T, U],
                          featureManager: RFFeatureManager,
                          dataIndex: RDD[StaticData[U]],
                          depthToStop: Int,
                          skip: RFSkip): ForestManager[T, U] = {
    val forestUpdated = super.executeSLC(forestManager, featureManager, dataIndex, depthToStop, skip)

    val t0 = System.currentTimeMillis()
    forestManager.merge(forestUpdated)
    val t1 = System.currentTimeMillis()
    println("SLC TIME MERGE FOREST: " + (t1 - t0))

    forestManager
  }
}