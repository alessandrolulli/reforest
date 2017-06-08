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

import reforest.rf.RFProperty

class MemoryUtil(val inputSize: Long, val property: RFProperty) extends Serializable {

  val memoryForWorkingDataPerNode = 1 + property.featureNumber + property.numTrees

  val totalMemoryForWorkingData = memoryForWorkingDataPerNode * inputSize

  val totalMemoryForWorkingDataPerWorker = totalMemoryForWorkingData / property.sparkExecutorInstances
  /*
  memory in bytes to store data for a node + DataOnWorker.nodeToArrayOffset (to save the offset of the bins)
   */
  val memoryPerNodeInMatrix = property.strategyFeature.getFeaturePerNodeNumber * property.numClasses * property.binNumber * 4 + (property.binNumber + 1) * 4

  /*
  featureMap: Broadcast[Map[(Int, Int), Array[Int]]]
   */
  val memoryForFeaturesPerNode = property.strategyFeature.getFeaturePerNodeNumber * 4 + 8

  /*
  idTOid: Broadcast[BiMap[(Int, Int), Int]] (*2  because ia a bimap that saves both the directed and the inverted map)
   */
  val memoryForIdMappingPerNode = 12 * 2

  /*
  multiplicated by 2 to avoid RDD copy error + sizePerFeature
   */
  val memoryPerNode = (memoryPerNodeInMatrix + memoryForFeaturesPerNode + memoryForIdMappingPerNode + property.strategyFeature.getFeaturePerNodeNumber * 2 * 4) * 2

  val sparkTotalByteAvailablePlusSafety = (((property.sparkExecutorMemory.replaceAll("\\D+", "").toDouble * 1024 * 1024) * 0.6) * (if (property.binNumber >= 64) 0.6 else 0.8))

  val sparkTotalByteAvailablePlusSafetyMinusWorkingData = sparkTotalByteAvailablePlusSafety - totalMemoryForWorkingDataPerWorker

  val maximumConcurrentNodes = Math.max(1000, (sparkTotalByteAvailablePlusSafetyMinusWorkingData / memoryPerNode).toInt)

  val splitComputationPerFeature = (4 + 8) * 10000

  val maximumConcurrentNumberOfFeature = Math.max(100, (sparkTotalByteAvailablePlusSafety / (splitComputationPerFeature * 2)).toInt)

  // FCS ////////////////////////////////////////////////

  def fcsMaximumClusterConcurrent(numCluster: Int, depth : Int) :Int = {
    val concurrentBucket = Math.max(1, Math.floor(sparkTotalByteAvailablePlusSafetyMinusWorkingData / fcsSizeForCluster(depth)).toInt)

    if(sparkTotalByteAvailablePlusSafetyMinusWorkingData > totalMemoryForWorkingData)
      numCluster
    else if(fcsSizeForCluster(depth) * concurrentBucket > totalMemoryForWorkingData) {
      numCluster
    } else {
      concurrentBucket
    }
  }

  def fcsSizeForCluster(depth : Int) = fcsClusterSize() * fcsSizeForNode(depth) * property.fcsSafeMemoryMultiplier
  def fcsSizeForNode(depth : Int) = (totalMemoryForWorkingData / Math.pow(2, depth))
  def fcsClusterNumber(nodeNumber : Int) = Math.max(1, Math.ceil(nodeNumber / fcsClusterSize())).toInt
  def fcsClusterSize() =  property.fcsNodesPerCore * property.sparkCoresMax

  def switchToFCS(depth : Int, nodeNumber : Int) : Boolean = {
    if(property.fcsActiveForce) {
      true
    } else if(depth == 0) {
      println("BYTES AVAILABLE: "+sparkTotalByteAvailablePlusSafetyMinusWorkingData)
      println("BYTES FOR FCS: "+totalMemoryForWorkingData.toDouble)
      sparkTotalByteAvailablePlusSafetyMinusWorkingData > totalMemoryForWorkingData
    } else {
      val sizeForCluster = fcsSizeForCluster(depth)

      println("CLUSTER NUMBER: "+fcsClusterNumber(nodeNumber)+" SIZE: "+fcsClusterSize())
      println("BYTES AVAILABLE: "+sparkTotalByteAvailablePlusSafetyMinusWorkingData)
      println("BYTES FOR FCS AT LEAST: "+sizeForCluster)

      fcsClusterNumber(nodeNumber) < 300 && sparkTotalByteAvailablePlusSafetyMinusWorkingData > sizeForCluster
    }
  }
}
