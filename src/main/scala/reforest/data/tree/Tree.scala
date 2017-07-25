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

package reforest.data.tree

import reforest.TypeInfo
import reforest.data.{RawData, RawDataLabeled, WorkingData}
import reforest.rf.feature.RFFeatureSizer

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * A tree of the forest
  *
  * @tparam T raw data type
  * @tparam U working data type
  */
abstract class Tree[T, U]() extends Serializable {

  /**
    * Merge this tree with another tree. The merge is in place
    *
    * @param other_ another tree to merge with this
    */
  def merge(other_ : Tree[T, U]): Unit

  /**
    * It predicts the class label of the given element
    *
    * @param maxDepth the maximum depth to visit
    * @param data     the data to predict
    * @param typeInfo the type information of the war data
    * @return the predicted class label
    */
  def predict(maxDepth: Int, data: RawDataLabeled[T, U], typeInfo: TypeInfo[T]): Int = {
    predict(maxDepth, data.features, typeInfo)
  }

  /**
    * It predicts the class label of the given element
    *
    * @param maxDepth the maximum depth to visit
    * @param data     the data to predict
    * @param typeInfo the type information of the war data
    * @return the predicted class label
    */
  def predict(maxDepth: Int, data: RawData[T, U], typeInfo: TypeInfo[T]): Int = {
    predict(maxDepth, 0, data, typeInfo)
  }

  /**
    * It predicts the class label of the given element starting from a given node index
    *
    * @param maxDepth the maximum depth to visit
    * @param nodeId   the node index from which to start
    * @param data     the data to predict
    * @param typeInfo the type information of the war data
    * @return the predicted class label
    */
  def predict(maxDepth: Int, nodeId: Int, data: RawData[T, U], typeInfo: TypeInfo[T]): Int

  /**
    * It returns a list of nodes that still require to be processed
    *
    * @return a list of node that require computation
    */
  def getNodeToBeComputed: ListBuffer[Int] = {
    getNodeToBeComputed(new ListBuffer[Int]())
  }

  /**
    * It returns a list of nodes that still require to be processed
    *
    * @param toReturn_ the list to populate with additional nodes
    * @return a list of node that require computation
    */
  def getNodeToBeComputed(toReturn_ : ListBuffer[Int]): ListBuffer[Int] = {
    getNodeToBeComputed(0, toReturn_)
  }

  /**
    * It returns a list of nodes that still require to be processed
    *
    * @param nodeId_   the node index from which to start
    * @param toReturn_ the list to populate with additional nodes
    * @return a list of node that require computation
    */
  def getNodeToBeComputed(nodeId_ : Int, toReturn_ : ListBuffer[Int]): ListBuffer[Int]

  /**
    * It returns the level of the given node id
    *
    * @param nodeId_ the node index to check
    * @return the level in the tree treeId of the node nodeId
    */
  def getLevel(nodeId_ : Int): Int = {
    Math.floor(scala.math.log(nodeId_ + 1) / scala.math.log(2)).toInt
  }

  /**
    * To get the left child of a given node
    *
    * @param nodeId_ the node index
    * @return the left child of the node nodeId
    */
  def getLeftChild(nodeId_ : Int): Int

  /**
    * To get the right child of a given node
    *
    * @param nodeId_ the node index
    * @return the left child of the node nodeId
    */
  def getRightChild(nodeId_ : Int): Int

  /**
    * To get the parent of a given node
    *
    * @param nodeId_ the node index
    * @return the left child of the node nodeId
    */
  def getParent(nodeId_ : Int): Int = {
    if (nodeId_ == 0) 0 else (nodeId_ - 1) / 2
  }

  def getLabel(nodeId_ : Int): Int

  /**
    * Set the class label of the given node
    *
    * @param nodeId_ the node index for which setting the class label
    * @param label_  the label to set in the node nodeId
    */
  def setLabel(nodeId_ : Int, label_ : Int)

  /**
    * Get the split of a give node
    *
    * @param nodeId_ the node index for which getting the split
    * @return
    */
  def getSplit(nodeId_ : Int): Option[TCut[T, U]]

  /**
    * Set the split on the given node
    *
    * @param nodeId_ the node index for which setting the split
    * @param cut_    the split to set in the node nodeId
    */
  def setSplit(nodeId_ : Int, cut_ : CutDetailed[T, U]): Unit

  /**
    * Set the given node as a leaf
    *
    * @param nodeId_ the node index to set as a leaf
    */
  def setLeaf(nodeId_ : Int)

  /**
    * Check if the given node is a leaf
    *
    * @param nodeId_ the node index to check
    * @return true if the given node is a leaf
    */
  def isLeaf(nodeId_ : Int): Boolean

  /**
    * Check if the given node is defined
    *
    * @param nodeId_ the node index to check
    * @return true if the given node is defined
    */
  def isDefined(nodeId_ : Int): Boolean

  /**
    * It returns the current node index of the element passed as argument
    *
    * @param data     the data to navigate in the tree
    * @param typeInfo the type information of the working data
    * @return the deepest node index where the data contributes
    */
  def getCurrentNodeId(data: WorkingData[U], typeInfo: TypeInfo[U], featureSizer: RFFeatureSizer): Option[Int] = {
    getCurrentNodeId(0, data, typeInfo, featureSizer)
  }

  /**
    * It returns the current node index of the element passed as argument
    *
    * @param nodeId   the node index from where start the search
    * @param data     the data to navigate in the tree
    * @param typeInfo the type information of the working data
    * @return the deepest node index where the data contributes
    */
  def getCurrentNodeId(nodeId: Int, data: WorkingData[U], typeInfo: TypeInfo[U], featureSizer: RFFeatureSizer): Option[Int] = {
    if (isLeaf(nodeId)) {
      Option.empty
    } else {
      val split = getSplit(nodeId)
      if (split.isEmpty) {
        Some(nodeId)
      } else {
        val splitLeft = split.get.shouldGoLeftBin(data, typeInfo, featureSizer)

        if (splitLeft) {
          getCurrentNodeId(getLeftChild(nodeId), data, typeInfo, featureSizer)
        } else {
          getCurrentNodeId(getRightChild(nodeId), data, typeInfo, featureSizer)
        }
      }
    }
  }

  /**
    * Retrieve all the leaf in the tree
    * @return the set of leaf in the tree
    */
  def getAllLeaf: mutable.BitSet
}

/**
  * Utility to construct a tree
  */
object Tree {
  /**
    * Utility to construct a tree given its maximum depth
    * @param maxDepth the max depth that will be stored in the tree
    * @tparam T raw data type
    * @tparam U working data type
    * @return a newly generated tree
    */
  def build[T, U](maxDepth: Int): Tree[T, U] = {
    if (maxDepth <= 20) {
      new TreeFull[T, U](maxDepth)
    } else {
      new TreeSparse[T, U](maxDepth)
    }

  }
}