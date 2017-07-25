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

package reforest.util

import reforest.rf.RFSkip
import reforest.rf.parameter.RFParameter

class MemoryUtil(val inputSize: Long, val property: RFParameter) extends Serializable {

  val numBin = property.getMaxBinNumber
  val strategyFeature = property.strategyFeature

  val memoryForWorkingDataPerNode = 1 + property.numFeatures + property.getMinNumTrees

  val totalMemoryForWorkingData = memoryForWorkingDataPerNode * inputSize

  val totalMemoryForWorkingDataPerWorker = totalMemoryForWorkingData / property.sparkExecutorInstances
  /*
  memory in bytes to store data for a node + DataOnWorker.nodeToArrayOffset (to save the offset of the bins)
   */
  val memoryPerNodeInMatrix = strategyFeature.getFeaturePerNodeNumber * property.numClasses * numBin * 4 + (numBin + 1) * 4

  /*
  featureMap: Broadcast[Map[(Int, Int), Array[Int]]]
   */
  val memoryForFeaturesPerNode = strategyFeature.getFeaturePerNodeNumber * 4 + 8

  /*
  idTOid: Broadcast[BiMap[(Int, Int), Int]] (*2  because ia a bimap that saves both the directed and the inverted map)
   */
  val memoryForIdMappingPerNode = 12 * 2

  /*
  multiplicated by 2 to avoid RDD copy error + sizePerFeature
   */
  val memoryPerNode = (memoryPerNodeInMatrix + memoryForFeaturesPerNode + memoryForIdMappingPerNode + strategyFeature.getFeaturePerNodeNumber * 2 * 4) * 2

  val sparkTotalByteAvailablePlusSafety = (((property.sparkExecutorMemory.replaceAll("\\D+", "").toDouble * 1024 * 1024) * 0.6) * (if (property.getMaxBinNumber >= 64) 0.6 else 0.8))

  val sparkTotalByteAvailablePlusSafetyMinusWorkingData = sparkTotalByteAvailablePlusSafety - totalMemoryForWorkingDataPerWorker

  val matrixNumber = property.featureMultiplierPerNode.length * property.binNumber.length
  val maximumConcurrentNodesTotal = Math.max(1000, (sparkTotalByteAvailablePlusSafetyMinusWorkingData / memoryPerNode).toInt)

  private val maximumConcurrentNodes = maximumConcurrentNodesTotal

  println("MEMORY UTIL CONCURRENT NODES: " + maximumConcurrentNodes + " (MATRICES " + matrixNumber + ")")

  def getMaximumConcurrenNodes(property: RFParameter, skip: RFSkip): Int = {
    val maxNodesConcurrentConfig = property.maxNodesConcurrent
    Math.max(100, if (maxNodesConcurrentConfig < 100) maximumConcurrentNodes / (matrixNumber - skip.size) else maxNodesConcurrentConfig)
  }

  val splitComputationPerFeature = (4 + 8) * 10000

  val maximumConcurrentNumberOfFeature = Math.max(100, (sparkTotalByteAvailablePlusSafety / (splitComputationPerFeature * 2)).toInt)

  // SLC ////////////////////////////////////////////////

  def slcMaximumClusterConcurrent(numCluster: Int, depth: Int): Int = {
    val concurrentBucket = Math.max(1, Math.floor(sparkTotalByteAvailablePlusSafetyMinusWorkingData / slcSizeForCluster(depth)).toInt)

    if (sparkTotalByteAvailablePlusSafetyMinusWorkingData > totalMemoryForWorkingData)
      numCluster
    else if (slcSizeForCluster(depth) * concurrentBucket > totalMemoryForWorkingData) {
      numCluster
    } else {
      concurrentBucket
    }
  }

  def slcSizeForCluster(depth: Int) = slcClusterSize() * slcSizeForNode(depth) * property.slcSafeMemoryMultiplier

  def slcSizeForNode(depth: Int) = (totalMemoryForWorkingData / Math.pow(2, depth))

  def slcClusterNumber(nodeNumber: Int) = Math.max(1, Math.ceil(nodeNumber / slcClusterSize())).toInt

  def slcClusterSize() = 1 //property.slcNodesPerCore * property.sparkCoresMax

  def switchToSLC(depth: Int, nodeNumber: Int): Boolean = {
    if (property.slcActiveForce) {
      true
    } else if (depth == 0) {
      if (sparkTotalByteAvailablePlusSafetyMinusWorkingData > totalMemoryForWorkingData) {
        println("SLC BYTES AVAILABLE: " + sparkTotalByteAvailablePlusSafetyMinusWorkingData)
        println("SLC BYTES FOR FCS: " + totalMemoryForWorkingData.toDouble)
      }

      sparkTotalByteAvailablePlusSafetyMinusWorkingData > totalMemoryForWorkingData
    } else {
      val sizeForCluster = slcSizeForCluster(depth)

      if (sparkTotalByteAvailablePlusSafetyMinusWorkingData > totalMemoryForWorkingData) {
        println("SLC CLUSTER NUMBER: " + slcClusterNumber(nodeNumber) + " SIZE: " + slcClusterSize())
        println("SLC BYTES AVAILABLE: " + sparkTotalByteAvailablePlusSafetyMinusWorkingData)
        println("SLC BYTES FOR FCS AT LEAST: " + sizeForCluster)
      }

      sparkTotalByteAvailablePlusSafetyMinusWorkingData > totalMemoryForWorkingData
    }
  }
}
