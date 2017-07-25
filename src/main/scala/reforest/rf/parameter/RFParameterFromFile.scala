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

import reforest.rf.feature._
import reforest.rf.split.{RFStrategySplitDistribution, RFStrategySplitRandom}
import reforest.util.{CCProperties, CCPropertiesImmutable}

/**
  * It contains all the configuration of ReForeSt
  * @param property to load the property from file
  */
class RFParameterFromFile(val configFile : String) {

  val loader = new CCProperties("ReForeSt", configFile).load()
  val property = loader.getImmutable

  case object NumTrees extends RFParameterTypeArrayInt(Array(10))
  case object BinNumber extends RFParameterTypeArrayInt(Array(32))
  case object Depth extends RFParameterTypeArrayInt(Array(10))
  case object FeatureMultiplierPerNode extends RFParameterTypeArrayInt(Array(1))

  val parameter = RFParameterBuilder.apply
    .addParameter(RFParameterType.NumFeatures, getFeatureNumber)
    .addParameter(RFParameterType.NumClasses, loader.getInt("numClasses", RFParameterType.NumClasses.defaultValue))
    .addParameter(RFParameterType.NumTrees, loader.getIntArray("numTrees", RFParameterType.NumTrees.defaultValue.last))
    .addParameter(RFParameterType.Depth, loader.getIntArray("maxDepth", RFParameterType.Depth.defaultValue.last))
    .addParameter(RFParameterType.BinNumber, loader.getIntArray("binNumber", RFParameterType.BinNumber.defaultValue.last))
    .addParameter(RFParameterType.FeatureMultiplierPerNode, loader.getDoubleArray("featureMultiplierPerNode", RFParameterType.FeatureMultiplierPerNode.defaultValue.last))
    .addParameter(RFParameterType.NumRotations, loader.getInt("numRotation", RFParameterType.NumRotations.defaultValue))
    .addParameter(RFParameterType.RotationRandomSeed, loader.getInt("rotationRandomSeed", RFParameterType.RotationRandomSeed.defaultValue))
    .addParameter(RFParameterType.PoissonMean, loader.getDouble("poissonMean", RFParameterType.PoissonMean.defaultValue))
    .addParameter(RFParameterType.ModelSelectionEpsilon, loader.getDouble("modelSelectionEpsilon", RFParameterType.ModelSelectionEpsilon.defaultValue))
    .addParameter(RFParameterType.ModelSelectionEpsilonRemove, loader.getDouble("modelSelectionEpsilonRemove", RFParameterType.ModelSelectionEpsilonRemove.defaultValue))
    .addParameter(RFParameterType.SkipAccuracy, loader.getBoolean("skipAccuracy", RFParameterType.SkipAccuracy.defaultValue))
    .addParameter(RFParameterType.PermitSparseWorkingData, loader.getBoolean("permitSparseWorkingData", RFParameterType.PermitSparseWorkingData.defaultValue))
    .addParameter(RFParameterType.OutputTree, loader.getBoolean("outputTree", RFParameterType.OutputTree.defaultValue))
    .addParameter(RFParameterType.MaxNodesConcurrent, loader.getInt("maxNodesConcurrent", RFParameterType.MaxNodesConcurrent.defaultValue))
    .addParameter(RFParameterType.AppName, property.appName)
    .addParameter(RFParameterType.Category, property.category)
    .addParameter(RFParameterType.FileType, property.fileType)
    .addParameter(RFParameterType.LogStat, loader.getBoolean("logStats", RFParameterType.LogStat.defaultValue))
    .addParameter(RFParameterType.TestAll, loader.getBoolean("testAll", RFParameterType.TestAll.defaultValue))
    .addParameter(RFParameterType.SLCActive, loader.getBoolean("fcsActive", RFParameterType.SLCActive.defaultValue))
    .addParameter(RFParameterType.SLCActiveForce, loader.getBoolean("fcsActiveForce", RFParameterType.SLCActiveForce.defaultValue))
    .addParameter(RFParameterType.ModelSelection, loader.getBoolean("modelSelection", RFParameterType.ModelSelection.defaultValue))
    .addParameter(RFParameterType.Rotation, loader.getBoolean("rotation", RFParameterType.Rotation.defaultValue))
    .addParameter(RFParameterType.SLCSafeMemoryMultiplier, Math.max(1, loader.getDouble("fcsSafeMemoryMultiplier", RFParameterType.SLCSafeMemoryMultiplier.defaultValue)))
    .addParameter(RFParameterType.SLCNodesPerCore, Math.max(1, loader.getInt("fcsNodesPerCore", RFParameterType.SLCNodesPerCore.defaultValue)))
    .addParameter(RFParameterType.SLCDepth, loader.getInt("fcsDepth", RFParameterType.SLCDepth.defaultValue))
    .addParameter(RFParameterType.Dataset, property.dataset)
    .addParameter(RFParameterType.JarPath, RFParameterType.JarPath.defaultValue)
    .addParameter(RFParameterType.SparkMaster, property.sparkMaster)
    .addParameter(RFParameterType.SparkCoresMax, property.sparkCoresMax)
    .addParameter(RFParameterType.SparkExecutorInstances, property.sparkExecutorInstances)
    .addParameter(RFParameterType.SparkExecutorMemory, property.sparkExecutorMemory)
    .addParameter(RFParameterType.SparkPartition, property.sparkPartition)
    .addParameter(RFParameterType.SparkBlockManagerSlaveTimeoutMs, property.sparkBlockManagerSlaveTimeoutMs)
    .addParameter(RFParameterType.SparkShuffleManager, property.sparkShuffleManager)
    .addParameter(RFParameterType.SparkCompressionCodec, property.sparkCompressionCodec)
    .addParameter(RFParameterType.SparkShuffleConsolidateFiles, property.sparkShuffleConsolidateFiles)
    .addParameter(RFParameterType.SparkAkkaFrameSize, property.sparkAkkaFrameSize)
    .addParameter(RFParameterType.SparkDriverMaxResultSize, property.sparkDriverMaxResultSize)
    .addParameter(RFParameterType.SparkExecutorExtraClassPath, loader.get("sparkExecutorExtraClassPath", RFParameterType.SparkExecutorExtraClassPath.defaultValue))
    .addParameter(RFParameterType.Instrumented, property.instrumented)
    .addStrategyFeature(strategyFeature)
    .addStrategySplit(strategySplit)
    .build

  def getFeatureNumber = loader.getInt("numFeatures", 0)

  def strategyFeature = loader.get("strategyFeature", "sqrt").toLowerCase match {
    case "all" => new RFStrategyFeatureALL(getFeatureNumber)
    case "sqrt" => new RFStrategyFeatureSQRT(getFeatureNumber)
    case "sqrtsqrt" => new RFStrategyFeatureSQRTSQRT(getFeatureNumber)
    case "log2" => new RFStrategyFeatureLOG2(getFeatureNumber)
    case "onethird" => new RFStrategyFeatureONETHIRD(getFeatureNumber)
    case _ => new RFStrategyFeatureSQRT(getFeatureNumber)
  }
  def strategySplit = loader.get("strategySplit", "distribution").toLowerCase match {
    case "distribution" => new RFStrategySplitDistribution
    case "random" => new RFStrategySplitRandom
    case _ => new RFStrategySplitDistribution
  }
}

object RFParameterFromFile {
  def apply(configFile : String) = new RFParameterFromFile(configFile).parameter
}