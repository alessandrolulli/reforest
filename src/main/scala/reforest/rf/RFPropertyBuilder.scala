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
import reforest.rf.split.{RFStrategySplitDistribution}

class RFPropertyBuilder {
  var storageLevel = StorageLevel.MEMORY_AND_DISK

  var binNumber = 32
  var numTrees = 3
  var maxDepth = 3
  var numClasses = 2
  var featureNumber = 0
  var poissonMean = 1.0
  var fast = false
  var skipAccuracy = false
  var permitSparseWorkingData = false
  var outputTree = false
  var maxNodesConcurrent = -1

  // strategy type: reforest / rotation / rotationsqrt
  var strategy = "reforest"
  var strategyFeature = new RFStrategyFeatureSQRT(featureNumber)
  var strategySplit = new RFStrategySplitDistribution
  val uuid = java.util.UUID.randomUUID.toString
  var appName = "ReForeSt"

  // FCS
  var fcsActive = false
  var fcsActiveForce = false
  var fcsDepth = -1
  var fcsSafeMemoryMultiplier = 1.4
  var fcsNodesPerCore = 1
  var fcsCycleActivation = -1

  // ROTATION
  var numRotation = numTrees
  var rotationRandomSeed = 0

  def setAppName(name : String) = {
    appName = name
  }
}
