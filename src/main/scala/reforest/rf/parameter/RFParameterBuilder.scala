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

import reforest.rf.feature.{RFStrategyFeature, RFStrategyFeatureSQRT}
import reforest.rf.split._

/**
  * The builder for the configuration parameter
  */
class RFParameterBuilder(private val parameterMapArrayInt: scala.collection.mutable.Map[RFParameterTypeArrayInt, Array[Int]] = scala.collection.mutable.Map(),
                         private val parameterMapArrayDouble: scala.collection.mutable.Map[RFParameterTypeArrayDouble, Array[Double]] = scala.collection.mutable.Map(),
                         private val parameterMapInt: scala.collection.mutable.Map[RFParameterTypeInt, Int] = scala.collection.mutable.Map(),
                         private val parameterMapDouble: scala.collection.mutable.Map[RFParameterTypeDouble, Double] = scala.collection.mutable.Map(),
                         private val parameterMapBoolean: scala.collection.mutable.Map[RFParameterTypeBoolean, Boolean] = scala.collection.mutable.Map(),
                         private val parameterMapString: scala.collection.mutable.Map[RFParameterTypeString, String] = scala.collection.mutable.Map(),
                         private var strategyFeature: Option[RFStrategyFeature] = Option.empty,
                         private var strategySplit: RFStrategySplit = new RFStrategySplitDistribution(),
                         private var UUID: String = java.util.UUID.randomUUID.toString) {

  def getStrategyFeature = {
    if (strategyFeature.isDefined) {
      strategyFeature.get
    } else {
      new RFStrategyFeatureSQRT(parameterMapInt.getOrElse(RFParameterType.NumFeatures, RFParameterType.NumFeatures.defaultValue))
    }
  }

  def addStrategyFeature(strategyFeatureArgs: RFStrategyFeature) = {
    strategyFeature = Some(strategyFeatureArgs)
    this
  }

  def addStrategySplit(strategySplitArgs: RFStrategySplit) = {
    strategySplit = strategySplitArgs
    this
  }

  def addParameter(parameter: RFParameterTypeArrayInt, value: Array[Int]) = {
    parameterMapArrayInt(parameter) = value.toList.sorted.toArray
    this
  }

  def addParameter(parameter: RFParameterTypeArrayDouble, value: Array[Double]) = {
    parameterMapArrayDouble(parameter) = value.toList.sorted.toArray
    this
  }

  def addParameter(parameter: RFParameterTypeArrayInt, min: Int, max: Int, increment: Int = -1) = {
    parameterMapArrayInt(parameter) = genValues(min, max, increment)
    this
  }

  def addParameter(parameter: RFParameterTypeArrayInt, exactValue: Int) = {
    parameterMapArrayInt(parameter) = Array(exactValue)
    this
  }

  def addParameter(parameter: RFParameterTypeArrayDouble, exactValue: Double) = {
    parameterMapArrayDouble(parameter) = Array(exactValue)
    this
  }

  def addParameter(parameter: RFParameterTypeInt, exactValue: Int) = {
    parameterMapInt(parameter) = exactValue
    this
  }

  def addParameter(parameter: RFParameterTypeBoolean, exactValue: Boolean) = {
    parameterMapBoolean(parameter) = exactValue
    this
  }

  def addParameter(parameter: RFParameterTypeDouble, exactValue: Double) = {
    parameterMapDouble(parameter) = exactValue
    this
  }

  def addParameter(parameter: RFParameterTypeString, exactValue: String) = {
    parameterMapString(parameter) = exactValue
    this
  }

  def build: RFParameter = {
    new RFParameter(
      //int
      parameterMapInt.getOrElse(RFParameterType.NumFeatures, RFParameterType.NumFeatures.defaultValue),
      parameterMapInt.getOrElse(RFParameterType.NumClasses, RFParameterType.NumClasses.defaultValue),
      parameterMapInt.getOrElse(RFParameterType.NumRotations, RFParameterType.NumRotations.defaultValue),
      parameterMapInt.getOrElse(RFParameterType.RotationRandomSeed, RFParameterType.RotationRandomSeed.defaultValue),
      parameterMapInt.getOrElse(RFParameterType.SparkExecutorInstances, RFParameterType.SparkExecutorInstances.defaultValue),
      parameterMapInt.getOrElse(RFParameterType.SparkCoresMax, RFParameterType.SparkCoresMax.defaultValue),
      parameterMapInt.getOrElse(RFParameterType.SparkPartition, RFParameterType.SparkPartition.defaultValue),
      parameterMapInt.getOrElse(RFParameterType.MaxNodesConcurrent, RFParameterType.MaxNodesConcurrent.defaultValue),
      parameterMapInt.getOrElse(RFParameterType.SLCDepth, RFParameterType.SLCDepth.defaultValue),
      parameterMapInt.getOrElse(RFParameterType.SLCNodesPerCore, RFParameterType.SLCNodesPerCore.defaultValue),
      parameterMapInt.getOrElse(RFParameterType.SLCCycleActivation, RFParameterType.SLCCycleActivation.defaultValue),
      //array[int]
      parameterMapArrayInt.getOrElse(RFParameterType.NumTrees, RFParameterType.NumTrees.defaultValue),
      parameterMapArrayInt.getOrElse(RFParameterType.BinNumber, RFParameterType.BinNumber.defaultValue),
      parameterMapArrayInt.getOrElse(RFParameterType.Depth, RFParameterType.Depth.defaultValue),
      parameterMapArrayDouble.getOrElse(RFParameterType.FeatureMultiplierPerNode, RFParameterType.FeatureMultiplierPerNode.defaultValue),
      getStrategyFeature,
      strategySplit,
      //boolean
      parameterMapBoolean.getOrElse(RFParameterType.Instrumented, RFParameterType.Instrumented.defaultValue),
      parameterMapBoolean.getOrElse(RFParameterType.LogStat, RFParameterType.LogStat.defaultValue),
      parameterMapBoolean.getOrElse(RFParameterType.SkipAccuracy, RFParameterType.SkipAccuracy.defaultValue),
      parameterMapBoolean.getOrElse(RFParameterType.PermitSparseWorkingData, RFParameterType.PermitSparseWorkingData.defaultValue),
      parameterMapBoolean.getOrElse(RFParameterType.OutputTree, RFParameterType.OutputTree.defaultValue),
      parameterMapBoolean.getOrElse(RFParameterType.SLCActive, RFParameterType.SLCActive.defaultValue),
      parameterMapBoolean.getOrElse(RFParameterType.SLCActiveForce, RFParameterType.SLCActiveForce.defaultValue),
      parameterMapBoolean.getOrElse(RFParameterType.ModelSelection, RFParameterType.ModelSelection.defaultValue),
      parameterMapBoolean.getOrElse(RFParameterType.Rotation, RFParameterType.Rotation.defaultValue),
      parameterMapBoolean.getOrElse(RFParameterType.TestAll, RFParameterType.TestAll.defaultValue),
      //double
      parameterMapDouble.getOrElse(RFParameterType.PoissonMean, RFParameterType.PoissonMean.defaultValue),
      parameterMapDouble.getOrElse(RFParameterType.SLCSafeMemoryMultiplier, RFParameterType.SLCSafeMemoryMultiplier.defaultValue),
      parameterMapDouble.getOrElse(RFParameterType.ModelSelectionEpsilon, RFParameterType.ModelSelectionEpsilon.defaultValue),
      parameterMapDouble.getOrElse(RFParameterType.ModelSelectionEpsilonRemove, RFParameterType.ModelSelectionEpsilonRemove.defaultValue),
      //string
      parameterMapString.getOrElse(RFParameterType.AppName, RFParameterType.AppName.defaultValue),
      parameterMapString.getOrElse(RFParameterType.SparkDriverMaxResultSize, RFParameterType.SparkDriverMaxResultSize.defaultValue),
      parameterMapString.getOrElse(RFParameterType.SparkAkkaFrameSize, RFParameterType.SparkAkkaFrameSize.defaultValue),
      parameterMapString.getOrElse(RFParameterType.SparkShuffleConsolidateFiles, RFParameterType.SparkShuffleConsolidateFiles.defaultValue),
      parameterMapString.getOrElse(RFParameterType.SparkCompressionCodec, RFParameterType.SparkCompressionCodec.defaultValue),
      parameterMapString.getOrElse(RFParameterType.SparkShuffleManager, RFParameterType.SparkShuffleManager.defaultValue),
      parameterMapString.getOrElse(RFParameterType.SparkBlockManagerSlaveTimeoutMs, RFParameterType.SparkBlockManagerSlaveTimeoutMs.defaultValue),
      parameterMapString.getOrElse(RFParameterType.SparkExecutorMemory, RFParameterType.SparkExecutorMemory.defaultValue),
      parameterMapString.getOrElse(RFParameterType.SparkMaster, RFParameterType.SparkMaster.defaultValue),
      parameterMapString.getOrElse(RFParameterType.SparkExecutorExtraClassPath, RFParameterType.SparkExecutorExtraClassPath.defaultValue),
      parameterMapString.getOrElse(RFParameterType.JarPath, RFParameterType.JarPath.defaultValue),
      parameterMapString.getOrElse(RFParameterType.Dataset, RFParameterType.Dataset.defaultValue),
      UUID,
      parameterMapString.getOrElse(RFParameterType.Category, RFParameterType.Category.defaultValue),
      parameterMapString.getOrElse(RFParameterType.FileType, RFParameterType.FileType.defaultValue)
    )
  }

  private[parameter] def genValues(min: Int, max: Int, increment: Int = -1) = {
    if (increment == -1) {
      Stream.iterate(min)(i => i + i).takeWhile(i => i < max + 1).toArray
    } else {
      Range(min, max + 1, increment).toArray
    }
  }
}

object RFParameterBuilder {
  def apply = new RFParameterBuilder

  def apply(parameter: RFParameter): RFParameterBuilder = {
    val parameterMapArrayInt: scala.collection.mutable.Map[RFParameterTypeArrayInt, Array[Int]] = scala.collection.mutable.Map()
    val parameterMapArrayDouble: scala.collection.mutable.Map[RFParameterTypeArrayDouble, Array[Double]] = scala.collection.mutable.Map()
    val parameterMapInt: scala.collection.mutable.Map[RFParameterTypeInt, Int] = scala.collection.mutable.Map()
    val parameterMapDouble: scala.collection.mutable.Map[RFParameterTypeDouble, Double] = scala.collection.mutable.Map()
    val parameterMapBoolean: scala.collection.mutable.Map[RFParameterTypeBoolean, Boolean] = scala.collection.mutable.Map()
    val parameterMapString: scala.collection.mutable.Map[RFParameterTypeString, String] = scala.collection.mutable.Map()
    var strategyFeature: Option[RFStrategyFeature] = Some(parameter.strategyFeature)
    var strategySplit: RFStrategySplit = parameter.strategySplit

    parameterMapInt.put(RFParameterType.NumFeatures, parameter.numFeatures)
    parameterMapInt.put(RFParameterType.NumClasses, parameter.numClasses)
    parameterMapInt.put(RFParameterType.NumRotations, parameter.numRotation)
    parameterMapInt.put(RFParameterType.RotationRandomSeed, parameter.rotationRandomSeed)
    parameterMapInt.put(RFParameterType.SparkExecutorInstances, parameter.sparkExecutorInstances)
    parameterMapInt.put(RFParameterType.SparkCoresMax, parameter.sparkCoresMax)
    parameterMapInt.put(RFParameterType.SparkPartition, parameter.sparkPartition)
    parameterMapInt.put(RFParameterType.MaxNodesConcurrent, parameter.maxNodesConcurrent)
    parameterMapInt.put(RFParameterType.SLCDepth, parameter.slcDepth)
    parameterMapInt.put(RFParameterType.SLCNodesPerCore, parameter.slcNodesPerCore)
    parameterMapInt.put(RFParameterType.SLCCycleActivation, parameter.slcCycleActivation)
    //array[int]
    parameterMapArrayInt.put(RFParameterType.NumTrees, parameter.numTrees)
    parameterMapArrayInt.put(RFParameterType.BinNumber, parameter.binNumber)
    parameterMapArrayInt.put(RFParameterType.Depth, parameter.depth)
    parameterMapArrayDouble.put(RFParameterType.FeatureMultiplierPerNode, parameter.featureMultiplierPerNode)
    //boolean
    parameterMapBoolean.put(RFParameterType.Instrumented, parameter.Instrumented)
    parameterMapBoolean.put(RFParameterType.LogStat, parameter.logStats)
    parameterMapBoolean.put(RFParameterType.SkipAccuracy, parameter.skipAccuracy)
    parameterMapBoolean.put(RFParameterType.PermitSparseWorkingData, parameter.permitSparseWorkingData)
    parameterMapBoolean.put(RFParameterType.OutputTree, parameter.outputTree)
    parameterMapBoolean.put(RFParameterType.SLCActive, parameter.slcActive)
    parameterMapBoolean.put(RFParameterType.SLCActiveForce, parameter.slcActiveForce)
    parameterMapBoolean.put(RFParameterType.ModelSelection, parameter.modelSelection)
    parameterMapBoolean.put(RFParameterType.Rotation, parameter.rotation)
    parameterMapBoolean.put(RFParameterType.TestAll, parameter.testAll)
    //double
    parameterMapDouble.put(RFParameterType.PoissonMean, parameter.poissonMean)
    parameterMapDouble.put(RFParameterType.SLCSafeMemoryMultiplier, parameter.slcSafeMemoryMultiplier)
    parameterMapDouble.put(RFParameterType.ModelSelectionEpsilon, parameter.modelSelectionEpsilon)
    parameterMapDouble.put(RFParameterType.ModelSelectionEpsilonRemove, parameter.modelSelectionEpsilonRemove)
    //string
    parameterMapString.put(RFParameterType.AppName, parameter.appName)
    parameterMapString.put(RFParameterType.SparkDriverMaxResultSize, parameter.sparkDriverMaxResultSize)
    parameterMapString.put(RFParameterType.SparkAkkaFrameSize, parameter.sparkAkkaFrameSize)
    parameterMapString.put(RFParameterType.SparkShuffleConsolidateFiles, parameter.sparkShuffleConsolidateFiles)
    parameterMapString.put(RFParameterType.SparkCompressionCodec, parameter.sparkCompressionCodec)
    parameterMapString.put(RFParameterType.SparkShuffleManager, parameter.sparkShuffleManager)
    parameterMapString.put(RFParameterType.SparkBlockManagerSlaveTimeoutMs, parameter.sparkBlockManagerSlaveTimeoutMs)
    parameterMapString.put(RFParameterType.SparkExecutorMemory, parameter.sparkExecutorMemory)
    parameterMapString.put(RFParameterType.SparkMaster, parameter.sparkMaster)
    parameterMapString.put(RFParameterType.SparkExecutorExtraClassPath, parameter.sparkExecutorExtraClassPath)
    parameterMapString.put(RFParameterType.JarPath, parameter.jarPath)
    parameterMapString.put(RFParameterType.Dataset, parameter.dataset)
    parameterMapString.put(RFParameterType.Category, parameter.category)
    parameterMapString.put(RFParameterType.FileType, parameter.fileType)

    new RFParameterBuilder(parameterMapArrayInt,
      parameterMapArrayDouble,
      parameterMapInt,
      parameterMapDouble,
      parameterMapBoolean,
      parameterMapString,
      strategyFeature,
      strategySplit,
      parameter.UUID)
  }

}
