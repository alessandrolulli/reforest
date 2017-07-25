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

package reforest.rf.parameter

import org.apache.spark.storage.StorageLevel
import reforest.rf.feature.RFStrategyFeature
import reforest.rf.split.RFStrategySplit

/**
  * The configuration parameter
  *
  * @param numFeatures
  * @param numClasses
  * @param numRotation
  * @param rotationRandomSeed
  * @param sparkExecutorInstances
  * @param sparkCoresMax
  * @param sparkPartition
  * @param maxNodesConcurrent
  * @param slcDepth
  * @param slcNodesPerCore
  * @param slcCycleActivation
  * @param numTrees
  * @param binNumber
  * @param depth
  * @param featureMultiplierPerNode
  * @param strategyFeature
  * @param strategySplit
  * @param Instrumented
  * @param logStats
  * @param skipAccuracy
  * @param permitSparseWorkingData
  * @param outputTree
  * @param slcActive
  * @param slcActiveForce
  * @param modelSelection
  * @param rotation
  * @param poissonMean
  * @param slcSafeMemoryMultiplier
  * @param modelSelectionEpsilon
  * @param modelSelectionEpsilonRemove
  * @param appName
  * @param sparkDriverMaxResultSize
  * @param sparkAkkaFrameSize
  * @param sparkShuffleConsolidateFiles
  * @param sparkCompressionCodec
  * @param sparkShuffleManager
  * @param sparkBlockManagerSlaveTimeoutMs
  * @param sparkExecutorMemory
  * @param sparkMaster
  * @param jarPath
  * @param dataset
  * @param UUID
  * @param category
  * @param fileType
  */
class RFParameter(val numFeatures: Int,
                  val numClasses: Int,
                  val numRotation: Int,
                  val rotationRandomSeed: Int,
                  val sparkExecutorInstances: Int,
                  val sparkCoresMax: Int,
                  val sparkPartition: Int,
                  val maxNodesConcurrent: Int,
                  val slcDepth: Int,
                  val slcNodesPerCore: Int,
                  val slcCycleActivation: Int,
                  val numTrees: Array[Int],
                  val binNumber: Array[Int],
                  val depth: Array[Int],
                  val featureMultiplierPerNode: Array[Double],
                  val strategyFeature: RFStrategyFeature,
                  val strategySplit: RFStrategySplit,
                  val Instrumented: Boolean,
                  val logStats: Boolean,
                  val skipAccuracy: Boolean,
                  val permitSparseWorkingData: Boolean,
                  val outputTree: Boolean,
                  val slcActive: Boolean,
                  val slcActiveForce: Boolean,
                  val modelSelection: Boolean,
                  val rotation: Boolean,
                  val testAll: Boolean,
                  val poissonMean: Double,
                  val slcSafeMemoryMultiplier: Double,
                  val modelSelectionEpsilon: Double,
                  val modelSelectionEpsilonRemove: Double,
                  val appName: String,
                  val sparkDriverMaxResultSize: String,
                  val sparkAkkaFrameSize: String,
                  val sparkShuffleConsolidateFiles: String,
                  val sparkCompressionCodec: String,
                  val sparkShuffleManager: String,
                  val sparkBlockManagerSlaveTimeoutMs: String,
                  val sparkExecutorMemory: String,
                  val sparkMaster: String,
                  val sparkExecutorExtraClassPath: String,
                  val jarPath: String,
                  val dataset: String,
                  val UUID: String,
                  val category: String,
                  val fileType: String
                 ) extends Serializable {

  val storageLevel = StorageLevel.MEMORY_AND_DISK

  def getSparkPartitionSLC(activeNodes: Int): Int = {
    Math.min(sparkPartition * 2, activeNodes)
  }

  def getMinNumTrees = numTrees.head

  def getMaxNumTrees = numTrees.last

  def getMaxBinNumber = binNumber.last

  def getMaxDepth = depth.last

  def getMaxFeatureMultiplierPerNode = featureMultiplierPerNode.last

  def getStrategyFeature = strategyFeature

  val numForest = binNumber.length * featureMultiplierPerNode.length

  def applyNumTrees(numTreesArgs: Int) = {
    if (numTrees.length == 1 && getMaxNumTrees == numTreesArgs) {
      this
    } else {
      val builder = RFParameterBuilder(this)
      builder.addParameter(RFParameterType.NumTrees, numTreesArgs)

      builder.build
    }
  }

  def applyAppName(name: String) = {
    val builder = RFParameterBuilder(this)
    builder.addParameter(RFParameterType.AppName, name)

    builder.build
  }

  def applySLCActive(slc: Boolean) = {
    val builder = RFParameterBuilder(this)
    builder.addParameter(RFParameterType.SLCActive, slc)

    builder.build
  }

  def applyRotation(rotation: Boolean) = {
    val builder = RFParameterBuilder(this)
    builder.addParameter(RFParameterType.Rotation, rotation)

    builder.build
  }

  def applyModelSelection(ms: Boolean) = {
    val builder = RFParameterBuilder(this)
    builder.addParameter(RFParameterType.ModelSelection, ms)

    builder.build
  }


  def applyMinAllowedDepth(d: Int) = {
    val depthFiltered = depth.filter(v => v >= d)
    val builder = RFParameterBuilder(this)
    builder.addParameter(RFParameterType.Depth, depthFiltered)

    builder.build
  }

  def applyDepth(d: Int) = {
    val builder = RFParameterBuilder(this)
    builder.addParameter(RFParameterType.Depth, d)

    builder.build
  }

  def applyBinNumber(d: Int) = {
    val builder = RFParameterBuilder(this)
    builder.addParameter(RFParameterType.BinNumber, d)

    builder.build
  }

  def applyFeatureMultiplier(d: Double) = {
    val builder = RFParameterBuilder(this)
    builder.addParameter(RFParameterType.FeatureMultiplierPerNode, d)

    builder.build
  }

}
