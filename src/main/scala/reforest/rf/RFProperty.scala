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
class RFProperty(val property : CCPropertiesImmutable) extends Serializable {
  val storageLevel = StorageLevel.MEMORY_AND_DISK

  val binNumber = property.loader.getInt("binNumber", 32)
  val numTrees = property.loader.getInt("numTrees", 3)
  val maxDepth = property.loader.getInt("maxDepth", 3)
  val numClasses = property.loader.getInt("numClasses", 2)
  val featureNumber = property.loader.getInt("numFeatures", 0)
  val poissonMean = property.loader.getDouble("poissonMean", 1.0)
  val fast = property.loader.getBoolean("fast", false)
  val skipAccuracy = property.loader.getBoolean("skipAccuracy", false)
  val permitSparseWorkingData = property.loader.getBoolean("permitSparseWorkingData", false)
  val outputTree = property.loader.getBoolean("outputTree", false)
  var maxNodesConcurrent = property.loader.getInt("maxNodesConcurrent", -1)

  // strategy type: reforest / rotation / rotationsqrt
  val strategy = property.loader.get("strategy", "reforest").toLowerCase
  val strategyFeature = property.loader.get("strategyFeature", "sqrt").toLowerCase match {
    case "all" => new RFStrategyFeatureALL(featureNumber)
    case "sqrt" => new RFStrategyFeatureSQRT(featureNumber)
    case "sqrtsqrt" => new RFStrategyFeatureSQRTSQRT(featureNumber)
    case "log2" => new RFStrategyFeatureLOG2(featureNumber)
    case "onethird" => new RFStrategyFeatureONETHIRD(featureNumber)
    case _ => new RFStrategyFeatureSQRT(featureNumber)
  }
  val strategySplit = property.loader.get("strategySplit", "distribution").toLowerCase match {
    case "distribution" => new RFStrategySplitDistribution
    case "random" => new RFStrategySplitRandom
    case _ => new RFStrategySplitDistribution
  }
  val uuid = java.util.UUID.randomUUID.toString
  var appName = property.appName

  val util = new CCUtil(this)
  val loader = property.loader

  // FCS
  val fcsActive = property.loader.getBoolean("fcsActive", false)
  val fcsActiveForce = property.loader.getBoolean("fcsActiveForce", false)
  val fcsDepth = property.loader.getInt("fcsDepth", -1)
  val fcsSafeMemoryMultiplier = Math.max(1, property.loader.getDouble("fcsSafeMemoryMultiplier", 1.4))
  val fcsNodesPerCore = Math.max(1, property.loader.getInt("fcsNodesPerCore", 1))
  var fcsCycleActivation = -1

  // ROTATION
  val numRotation = property.loader.getInt("numRotation", numTrees)
  val rotationRandomSeed = property.loader.getInt("rotationRandomSeed", 0)

  def setAppName(name : String) = {
    appName = name
  }
}
