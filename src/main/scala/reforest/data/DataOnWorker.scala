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

package reforest.data

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import reforest.rf.RFFeatureSizer
import reforest.rf.split.RFSplitterManager
import reforest.util.BiMap

import scala.collection.Map

object DataOnWorker extends Serializable {

  var iterationInit = (-1, -1)
  var initBy = -1
  var array: Array[Array[Int]] = Array.empty
  var arrayLength : Int = 0
  var sizePerFeature: Option[RFFeatureSizer] = Option.empty
  var nodeToArrayOffset: Array[Array[Int]] = Array.empty

  def sendWorkingData = {
    var toReturn : Option[Array[Array[Int]]] = Option.empty
    this.synchronized({
      if(array.length > 0) {
        toReturn = Some(array)
        array = Array.empty
      }
    })
    toReturn
  }

  def releaseMatrix = {
    this.synchronized({
      array = Array.empty
    })
  }

  def releaseOffset = {
    this.synchronized({
      nodeToArrayOffset = Array.empty
    })
  }

  def getOffset(idx: Int, featurePosition: Int): (Int, Int) = {
    (nodeToArrayOffset(idx)(featurePosition), nodeToArrayOffset(idx)(featurePosition + 1))
  }

  def getOffset(idx: Int): Array[Int] = {
    nodeToArrayOffset(idx)
  }

  def getColumn(position : Int, binNumber : Int, label: Int, bin: Int, numClasses: Int) : Int = {
    position * (binNumber+1) * numClasses + bin * numClasses + label
  }

  def getPositionOffset(offset: Int, label: Int, bin: Int, numClasses: Int): Int = {
    offset + bin * numClasses + label
  }

  def getPosition(idx: Int, featurePosition: Int, label: Int, bin: Int, numClasses: Int): Int = {
    nodeToArrayOffset(idx)(featurePosition) + bin * numClasses + label
  }

  def init[T, U](depth: Int, iteration: Int, iterationNumber: Int, nRows: Int,
                 numClasses: Int,
                 splitter: Broadcast[RFSplitterManager[T, U]],
                 featureMap: Broadcast[Map[(Int, Int), Array[Int]]],
                 idTOid: Broadcast[BiMap[(Int, Int), Int]]) = {
    this.synchronized(
      if (iterationInit != (depth, iteration)) {
        iterationInit = (depth, iteration)
        initBy = TaskContext.getPartitionId()

        if (sizePerFeature.isEmpty || (depth == 1 && iteration == 0)) sizePerFeature = Some(splitter.value.generateRFSizer(numClasses))

        val tmpMapOffset = featureMap.value.map{ case (treeNodeId, featureIdArray) => (idTOid.value(treeNodeId), {
          val arrayFeatureBinSize = featureIdArray.map(u => sizePerFeature.get.getSize(u))
          val toReturn = new Array[Int](arrayFeatureBinSize.length + 1)
          var c = 0
          var sum = 0
          while (c < toReturn.length) {
            toReturn(c) = sum
            if (c < arrayFeatureBinSize.length) {
              sum += arrayFeatureBinSize(c)
            }
            c += 1
          }

          toReturn
        })}

        var c = 0
        nodeToArrayOffset = new Array(nRows)
        while (c < nRows) {
          nodeToArrayOffset(c) = tmpMapOffset(c)
          c += 1
        }

        array = Array.tabulate(nRows)(t => new Array[Int]({
          nodeToArrayOffset(t).last
        }))
        arrayLength = array.length
      })
  }
}
