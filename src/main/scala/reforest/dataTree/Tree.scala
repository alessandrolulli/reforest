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

package reforest.dataTree

import reforest.TypeInfo
import reforest.data.{RawData, RawDataLabeled, WorkingData}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * A tree of the forest
  *
  * @param maxDepth the maximum allowed (configured) depth
  * @tparam T raw data type
  * @tparam U working data type
  */
class Tree[T, U](val maxDepth: Int) extends Serializable {
  private val _maxNodeNumber = Math.pow(2, maxDepth + 1).toInt - 1

  private var _label: Array[Int] = Array.tabulate(_maxNodeNumber)(_ => -1)
  private var _split: Array[Option[TCut[T, U]]] = Array.tabulate(_maxNodeNumber / 2)(_ => Option.empty)
  private var _leaf: mutable.BitSet = mutable.BitSet.empty

  override def toString = {
    var toReturn = ""
    var nodeId = 0
    while (nodeId < _maxNodeNumber) {
      if (_split(nodeId).isDefined || _label(nodeId) != -1) {
        val space = Array.fill[String](getLevel(nodeId))("\t").mkString("")
        toReturn += space + " " + nodeId + " " + (if (_leaf(nodeId)) "LEAF ") + _label(nodeId) + " " + _split(nodeId) + "\n"
      }
      nodeId += 1
    }

    toReturn
  }

  /**
    * Merge this tree with another tree. The merge is in place
    *
    * @param other_ another tree to merge with this
    */
  def merge(other_ : Tree[T, U]): Unit = {
    if (_split(0).isEmpty) {
      _label = other_._label
      _split = other_._split
      _leaf = other_._leaf
    } else {
      var count = 0
      while (count < _maxNodeNumber) {
        if (_label(count) < 0) {
          _label(count) = other_._label(count)
        }
        if (_split(count).isEmpty) {
          _split(count) = other_._split(count)
        }
        count += 1
      }
      _leaf.union(other_._leaf)
    }
  }

  /**
    * It predicts the class label of the given element
    *
    * @param data     the data to predict
    * @param typeInfo the type information of the war data
    * @return the predicted class label
    */
  def predict(data: RawDataLabeled[T, U], typeInfo: TypeInfo[T]): Int = {
    predict(data.features, typeInfo)
  }

  /**
    * It predicts the class label of the given element
    *
    * @param data     the data to predict
    * @param typeInfo the type information of the war data
    * @return the predicted class label
    */
  def predict(data: RawData[T, U], typeInfo: TypeInfo[T]): Int = {
    predict(0, data, typeInfo)
  }

  /**
    * It predicts the class label of the given element starting from a given node index
    *
    * @param nodeId   the node index from which to start
    * @param data     the data to predict
    * @param typeInfo the type information of the war data
    * @return the predicted class label
    */
  def predict(nodeId: Int, data: RawData[T, U], typeInfo: TypeInfo[T]): Int = {
    if (isLeaf(nodeId) || _split(nodeId).isEmpty) {
      _label(nodeId)
    } else {
      val splitLeft = _split(nodeId).get.shouldGoLeft(data, typeInfo)

      if (splitLeft) {
        predict(getLeftChild(nodeId), data, typeInfo)
      } else {
        predict(getRightChild(nodeId), data, typeInfo)
      }
    }
  }

  /**
    * It returns a list of nodes that still require to be processed
    *
    * @return a list of node that require computation
    */
  def getNodeToBeComputed(): ListBuffer[Int] = {
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
  def getNodeToBeComputed(nodeId_ : Int, toReturn_ : ListBuffer[Int]): ListBuffer[Int] = {
    if (_split(nodeId_).isEmpty) {
      toReturn_ += nodeId_
    } else {
      val leftChild = getLeftChild(nodeId_)
      if (leftChild != -1) {
        getNodeToBeComputed(leftChild, toReturn_)
      }
      val rightChild = getRightChild(nodeId_)
      if (rightChild != -1) {
        getNodeToBeComputed(rightChild, toReturn_)
      }
    }
    toReturn_
  }

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
  def getLeftChild(nodeId_ : Int): Int = {
    val child = nodeId_ * 2 + 1
    if (child < _maxNodeNumber) {
      child
    } else {
      -1
    }
  }

  /**
    * To get the right child of a given node
    *
    * @param nodeId_ the node index
    * @return the left child of the node nodeId
    */
  def getRightChild(nodeId_ : Int): Int = {
    val child = nodeId_ * 2 + 2
    if (child < _maxNodeNumber) {
      child
    } else {
      -1
    }
  }

  /**
    * To get the parent of a given node
    *
    * @param nodeId_ the node index
    * @return the left child of the node nodeId
    */
  def getParent(nodeId_ : Int): Int = {
    if (nodeId_ == 0) 0 else (nodeId_ - 1) / 2
  }

  /**
    * Set the class label of the given node
    *
    * @param nodeId_ the node index for which setting the class label
    * @param label_  the label to set in the node nodeId
    */
  def setLabel(nodeId_ : Int, label_ : Int) = {
    _label(nodeId_) = label_
  }

  /**
    * Get the split of a give node
    *
    * @param nodeId_ the node index for which getting the split
    * @return
    */
  def getSplit(nodeId_ : Int) = {
    _split(nodeId_)
  }

  /**
    * Set the split on the given node
    *
    * @param nodeId_ the node index for which setting the split
    * @param cut_    the split to set in the node nodeId
    */
  def setSplit(nodeId_ : Int, cut_ : CutDetailed[T, U]) = {
    _split(nodeId_) = Some(cut_.compress())
  }

  /**
    * Set the given node as a leaf
    *
    * @param nodeId_ the node index to set as a leaf
    */
  def setLeaf(nodeId_ : Int) = {
    _leaf += nodeId_
  }

  /**
    * Check if the given node is a leaf
    *
    * @param nodeId_ the node index to check
    * @return true if the given node is a leaf
    */
  def isLeaf(nodeId_ : Int): Boolean = {
    _leaf.contains(nodeId_)
  }

  /**
    * Check if the given node is defined
    *
    * @param nodeId_ the node index to check
    * @return true if the given node is defined
    */
  def isDefined(nodeId_ : Int): Boolean = {
    _label(nodeId_) != -1
  }

  /**
    * It returns the current node index of the element passed as argument
    *
    * @param data     the data to navigate in the tree
    * @param typeInfo the type information of the working data
    * @return the deepest node index where the data contributes
    */
  def getCurrentNodeId(data: WorkingData[U], typeInfo: TypeInfo[U]): Option[Int] = {
    getCurrentNodeId(0, data, typeInfo)
  }

  /**
    * It returns the current node index of the element passed as argument
    *
    * @param nodeId   the node index from where start the search
    * @param data     the data to navigate in the tree
    * @param typeInfo the type information of the working data
    * @return the deepest node index where the data contributes
    */
  def getCurrentNodeId(nodeId: Int, data: WorkingData[U], typeInfo: TypeInfo[U]): Option[Int] = {
    if (isLeaf(nodeId)) Option.empty
    else if (getSplit(nodeId).isEmpty) {
      Some(nodeId)
    } else {
      val splitLeft = getSplit(nodeId).get.shouldGoLeftBin(data, typeInfo)

      if (splitLeft) {
        getCurrentNodeId(getLeftChild(nodeId), data, typeInfo)
      } else {
        getCurrentNodeId(getRightChild(nodeId), data, typeInfo)
      }

    }
  }
}