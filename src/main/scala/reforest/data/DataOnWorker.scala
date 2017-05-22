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

/**
  * It is a singleton. It maintains in each machine the Matrix to collect the information to compute the best cuts.
  * In each iteration:
  *   - the first thread accessing the singleton initializes the Matrix
  *   - all the partitions concurrently update the matrix during the "Local Information Collection" step
  *   - during the "Distributed Information Aggregation" step the fist thread entering is the one in charge of sending
  * the data to the shuffling phase
  *   - the Matrix is freed
  */
object DataOnWorker extends Serializable {

  var initBy = -1
  var array: Array[Array[Int]] = Array.empty
  var arrayLength: Int = 0
  private var iterationInit = (-1, -1)
  private var sizePerFeature: Option[RFFeatureSizer] = Option.empty
  private var nodeToArrayOffset: Array[Array[Int]] = Array.empty

  def sendWorkingData = {
    var toReturn: Option[Array[Array[Int]]] = Option.empty
    this.synchronized({
      if (array.length > 0) {
        toReturn = Some(array)
        array = Array.empty
      }
    })
    toReturn
  }

  /**
    * It frees the memory relative to the matrix
    */
  def releaseMatrix = {
    this.synchronized({
      array = Array.empty
    })
  }

  /**
    * It returns the range of indices of a row of the matrix where the information of a feature are stored
    *
    * @param idx             the idx of a row in the matrix (each idx is relative to a specified node calculated in the iteration)
    * @param featurePosition the position of the feature to retrieve the range. The position must be
    *                        in [0, he number of features to use as candidates for splitting at each tree node]
    * @return the range of indices
    */
  def getOffset(idx: Int, featurePosition: Int): (Int, Int) = {
    (nodeToArrayOffset(idx)(featurePosition), nodeToArrayOffset(idx)(featurePosition + 1))
  }

  /**
    * It returns all the range of indices (for all the feature calculated on a given node)
    *
    * @param idx the idx of a row in the matrix (each idx is relative to a specified node calculated in the iteration)
    * @return all the range of indices for the node at index idx in the matrix
    */
  def getOffset(idx: Int): Array[Int] = {
    nodeToArrayOffset(idx)
  }

  /**
    * (this is not the correct position for this function)
    * It returns the position in the array in SLC mode to update
    *
    * @param position   the position of the feature in the array of selected features for the node
    * @param binNumber  the number of configured bin
    * @param label      the class label of the element
    * @param bin        the bin value of the element for the feature in position "position"
    * @param numClasses number of classes in the dataset
    * @return the position in the array to update
    */
  def getColumn(position: Int, binNumber: Int, label: Int, bin: Int, numClasses: Int): Int = {
    position * (binNumber + 1) * numClasses + bin * numClasses + label
  }

  /**
    * Given an offset it returns the position (in a row of the matrix)
    *
    * @param offset     the offset to add
    * @param label      the class label of the element
    * @param bin        the bin value of the element for the feature in position starting from "offset"
    * @param numClasses number of classes in the dataset
    * @return the position in the row of the matrix
    */
  def getPositionOffset(offset: Int, label: Int, bin: Int, numClasses: Int): Int = {
    offset + bin * numClasses + label
  }

  /**
    * to initialize the matrix in each iteration
    *
    * @param depth           the current depth processed
    * @param iteration       the sub-iteration currently processed
    * @param iterationNumber the number of sub-iterations
    * @param nRows           the number of rows to initialize in the matrix
    * @param numClasses      number of classes in the dataset
    * @param splitter        the utility to get the splits of each feature
    * @param featureMap      the selected features for each node processed in this sub-iteration
    * @param idTOid          a mappaing between (treeId,nodeId) to idx (row) in the matrix
    * @tparam T raw data type
    * @tparam U working data type
    */
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

        nodeToArrayOffset = new Array(nRows)

        featureMap.value.foreach { case (treeNodeId, featureIdArray) =>
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

          nodeToArrayOffset(idTOid.value(treeNodeId)) = toReturn
        }


        array = new Array[Array[Int]](nRows)
        var c = 0
        while (c < nRows) {
          array(c) = new Array[Int](nodeToArrayOffset(c).last)
          c += 1
        }

        arrayLength = nRows
      })
  }
}
