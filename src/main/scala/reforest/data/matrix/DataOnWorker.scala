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

package reforest.data.matrix

import reforest.data.tree.{Forest, NodeId}
import reforest.rf.feature.RFFeatureManager
import reforest.util.BiMap

/**
  *
  * @param depth           the current depth processed
  * @param iteration       the sub-iteration currently processed
  * @param iterationNumber the number of sub-iterations
  *                        //  * @param nRows           the number of rows to initialize in the matrix
  * @param numClasses      number of classes in the dataset
  *                        //  * @param splitter        the utility to get the splits of each feature
  * @param featureManager  the selected features for each node processed in this sub-iteration
  *                        //  * @param idTOid          a mappaing between (treeId,nodeId) to idx (row) in the matrix
  */
class DataOnWorker(forestId: Int,
                   //                   nRows: Int,
                   numClasses: Int,
                   featureManager: RFFeatureManager) extends Serializable {
  var array: Array[Array[Int]] = Array.empty
  var arrayLength: Int = 0
  private var nodeToArrayOffset: Array[Array[Int]] = Array.empty

  /**
    * It frees the memory relative to the matrix
    */
  def releaseMatrix(): Unit = {
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
    * to initialize the matrix in each iteration
    */
  def init[T, U](forest: Forest[T, U], idToId: BiMap[NodeId, Int], iteration: Int, iterationNumber: Int, depth: Int): Unit = {
    this.synchronized({
      if (array.isEmpty) {
        val featureForest = if (iterationNumber > 1) {
          featureManager.getFeatureForForest(forestId).filter(n => n._1.treeId % iterationNumber == iteration)
        } else {
          featureManager.getFeatureForForest(forestId)
        }

        arrayLength = featureForest.size
        nodeToArrayOffset = new Array(arrayLength)
        array = new Array[Array[Int]](arrayLength)

        featureForest.foreach { case (nodeId, featureIdArray) =>
          val arrayFeatureBinSize = featureIdArray.map(u => forest.featureSizer.getSize(u))
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

          val idx = idToId(nodeId)
          nodeToArrayOffset(idx) = toReturn
          array(idx) = new Array[Int](toReturn.last)
        }
      }
    })
  }
}

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
}
