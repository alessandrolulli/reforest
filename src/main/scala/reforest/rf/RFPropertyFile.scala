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

import org.apache.spark.storage.StorageLevel
import reforest.rf.split.{RFStrategySplitDistribution, RFStrategySplitRandom}
import reforest.util.{CCPropertiesImmutable, CCUtil}

/**
  * It contains all the configuration of ReForeSt
  * @param property to load the property from file
  */
class RFPropertyFile(val property : CCPropertiesImmutable) extends RFProperty {
  val loader = property.loader

  storageLevel = StorageLevel.MEMORY_AND_DISK

  binNumber = loader.getInt("binNumber", 32)
  numTrees = loader.getInt("numTrees", 3)
  maxDepth = loader.getInt("maxDepth", 3)
  numClasses = loader.getInt("numClasses", 2)
  featureNumber = loader.getInt("numFeatures", 0)
  poissonMean = loader.getDouble("poissonMean", 1.0)
  fast = loader.getBoolean("fast", false)
  skipAccuracy = loader.getBoolean("skipAccuracy", false)
  permitSparseWorkingData = loader.getBoolean("permitSparseWorkingData", false)
  outputTree = loader.getBoolean("outputTree", false)
  maxNodesConcurrent = loader.getInt("maxNodesConcurrent", -1)

  // strategy type: reforest / rotation / rotationsqrt
  strategy = loader.get("strategy", "reforest").toLowerCase
  override def strategyFeature = loader.get("strategyFeature", "sqrt").toLowerCase match {
    case "all" => new RFStrategyFeatureALL(featureNumber)
    case "sqrt" => new RFStrategyFeatureSQRT(featureNumber)
    case "sqrtsqrt" => new RFStrategyFeatureSQRTSQRT(featureNumber)
    case "log2" => new RFStrategyFeatureLOG2(featureNumber)
    case "onethird" => new RFStrategyFeatureONETHIRD(featureNumber)
    case _ => new RFStrategyFeatureSQRT(featureNumber)
  }
  strategySplit = loader.get("strategySplit", "distribution").toLowerCase match {
    case "distribution" => new RFStrategySplitDistribution
    case "random" => new RFStrategySplitRandom
    case _ => new RFStrategySplitDistribution
  }
  appName = property.appName

  category = property.category
  fileType = property.fileType

  // FCS
  fcsActive = loader.getBoolean("fcsActive", false)
  fcsActiveForce = loader.getBoolean("fcsActiveForce", false)
  fcsDepth = loader.getInt("fcsDepth", -1)
  fcsSafeMemoryMultiplier = Math.max(1, loader.getDouble("fcsSafeMemoryMultiplier", 1.4))
  fcsNodesPerCore = Math.max(1, loader.getInt("fcsNodesPerCore", 1))
  fcsCycleActivation = -1

  // ROTATION
  numRotation = loader.getInt("numRotation", numTrees)
  rotationRandomSeed = loader.getInt("rotationRandomSeed", 0)

  // SPARK
  dataset = property.dataset
  outputFile = property.outputFile
  jarPath = property.outputFile
  sparkMaster = property.sparkMaster
  sparkCoresMax = property.sparkCoresMax
  sparkExecutorInstances = property.sparkExecutorInstances
  sparkExecutorMemory = property.sparkExecutorMemory
  sparkPartition = property.sparkPartition
  sparkBlockManagerSlaveTimeoutMs = property.sparkBlockManagerSlaveTimeoutMs
  sparkShuffleManager = property.sparkShuffleManager
  sparkCompressionCodec = property.sparkCompressionCodec
  sparkShuffleConsolidateFiles = property.sparkShuffleConsolidateFiles
  sparkAkkaFrameSize = property.sparkAkkaFrameSize
  sparkDriverMaxResultSize = property.sparkDriverMaxResultSize

  separator = property.separator
  instrumented = property.instrumented
}
