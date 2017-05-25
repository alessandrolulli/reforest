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
import reforest.data.{RawData, WorkingData}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * A base class to construct a forest
  *
  * @param numTrees the total number of trees in the forest
  * @param maxDepth the maximum allowed depth (from configuration)
  * @tparam T raw data type
  * @tparam U working data type
  */
abstract class Forest[T, U](val numTrees: Int, maxDepth: Int) extends Serializable {

  /**
    * Get all the trees of the forest
    *
    * @return all the trees
    */
  def getTree: Array[Tree[T, U]]

  /**
    * Check if a tree is active (initialized)
    *
    * @param treeId
    * @return
    */
  def isActive(treeId: Int): Boolean

  override def toString() = {
    var toReturn = ""
    var treeId = 0
    while (treeId < numTrees) {
      if(isActive(treeId)) {
        toReturn += "TREE ID: " + treeId + "\n" + getTree(treeId).toString + "\n\n"
      }
      treeId += 1
    }
    toReturn
  }

  /**
    * Merge with another forest
    *
    * @param other an another forest
    */
  def merge(other: Forest[T, U]) : Unit

  /**
    * It predicts the class label of the given element
    *
    * @param treeId   the index of the tree to use for prediction
    * @param data     the data to predict
    * @param typeInfo the type information of the war data
    * @return the predicted class label
    */
  def predict(treeId: Int, data: RawData[T, U], typeInfo: TypeInfo[T]) = {
    getTree(treeId).predict(data, typeInfo)
  }

  /**
    * It returns the level of the given node id the the given tree
    *
    * @param treeId the tree index
    * @param nodeId the node index to check
    * @return the level in the tree treeId of the node nodeId
    */
  def getLevel(treeId: Int, nodeId: Int) = {
    getTree(treeId).getLevel(nodeId)
  }

  /**
    * To get the left child of a given node
    *
    * @param treeId the tree index
    * @param nodeId the node index
    * @return the left child of the node nodeId
    */
  def getLeftChild(treeId: Int, nodeId: Int): Int = {
    getTree(treeId).getLeftChild(nodeId)
  }

  /**
    * To get the right child of a given node
    *
    * @param treeId the tree index
    * @param nodeId the node index
    * @return the right child of the node nodeId
    */
  def getRightChild(treeId: Int, nodeId: Int): Int = {
    getTree(treeId).getRightChild(nodeId)
  }

  /**
    * To get the parent of a given node. For the root it return the same nodeId
    *
    * @param treeId the tree index
    * @param nodeId the node index
    * @return the parent of the node nodeId
    */
  def getParent(treeId: Int, nodeId: Int): Int = {
    getTree(treeId).getParent(nodeId)
  }

  /**
    * Set the class label of the given node
    *
    * @param treeId the tree index
    * @param nodeId the node index for which setting the class label
    * @param label  the label to set in the node nodeId
    */
  def setLabel(treeId: Int, nodeId: Int, label: Int) = {
    getTree(treeId).setLabel(nodeId, label)
  }

  /**
    * Set the split on the given node
    *
    * @param treeId the tree index
    * @param nodeId the node index for which setting the split
    * @param cut    the split to set in the node nodeId
    */
  def setSplit(treeId: Int, nodeId: Int, cut: CutDetailed[T, U]) = {
    getTree(treeId).setSplit(nodeId, cut)
  }

  /**
    * Set the given node as a leaf
    *
    * @param treeId the tree index
    * @param nodeId the node index to set as a leaf
    */
  def setLeaf(treeId: Int, nodeId: Int): Unit = {
    getTree(treeId).setLeaf(nodeId)
  }

  /**
    * Check if the given node is a leaf
    *
    * @param treeId the tree index
    * @param nodeId the node index to check
    * @return true if the given node is a leaf
    */
  def isLeaf(treeId: Int, nodeId: Int): Boolean = {
    getTree(treeId).isLeaf(nodeId)
  }

  /**
    * Check if the given node is defined
    *
    * @param treeId the tree index
    * @param nodeId the node index to check
    * @return true if the given node is defined
    */
  def isDefined(treeId: Int, nodeId: Int): Boolean = {
    getTree(treeId).isDefined(nodeId)
  }

  /**
    * It returns the current node index of the element passed as argument
    *
    * @param treeId   the tree index
    * @param data     the data to navigate in the tree
    * @param typeInfo the type information of the working data
    * @return the deepest node index where the data contributes
    */
  def getCurrentNodeId(treeId: Int, data: WorkingData[U], typeInfo: TypeInfo[U]): Option[Int] = {
    getTree(treeId).getCurrentNodeId(data, typeInfo)
  }

  /**
    * It returns the current node index of the element passed as argument starting from the node index nodeId
    *
    * @param treeId   the tree index
    * @param nodeId   the node index for starting the navigation in the tree treeId
    * @param data     the data to navigate in the tree
    * @param typeInfo the type information of the working data
    * @return the deepest node index where the data contributes
    */
  def getCurrentNodeId(treeId: Int, nodeId: Int, data: WorkingData[U], typeInfo: TypeInfo[U]): Option[Int] = {
    getTree(treeId).getCurrentNodeId(nodeId, data, typeInfo)
  }

  /**
    * It returns a list of nodes that still require to be processed
    *
    * @return a list of node that require computation
    */
  def getNodeToBeComputed(): ListBuffer[(Int, Int)] = {
    val toReturn = new ListBuffer[(Int, Int)]()
    var treeId = 0
    while (treeId < numTrees) {
      toReturn ++= getTree(treeId).getNodeToBeComputed().map(nodeId => (treeId, nodeId))
      treeId += 1
    }
    toReturn
  }
}

/**
  * A forest where all the trees are initialized
  *
  * @param numTrees the total number of trees in the forest
  * @param maxDepth the maximum allowed depth (from configuration)
  * @tparam T raw data type
  * @tparam U working data type
  */
class ForestFull[T, U](numTrees: Int, maxDepth: Int) extends Forest[T, U](numTrees, maxDepth) {
  private val tree = Array.tabulate(numTrees)(_ => new Tree[T, U](maxDepth))

  override def getTree = tree

  override def isActive(treeId: Int): Boolean = true

  override def merge(other: Forest[T, U]) : Unit = {
    var count = 0
    while (count < numTrees) {
      if (other.isActive(count)) {
        if (isActive(count)) {
          tree(count).merge(other.getTree(count))
        } else {
          tree(count) = other.getTree(count)
        }
      }
      count += 1
    }
  }
}

/**
  * A forest where at the beginning no trees are initialized. It supports incremental addition of trees
  *
  * @param numTrees the total number of trees in the forest
  * @param maxDepth the maximum allowed depth (from configuration)
  * @tparam T raw data type
  * @tparam U working data type
  */
class ForestIncremental[T, U](numTrees: Int, maxDepth: Int) extends Forest[T, U](numTrees, maxDepth) {
  private val tree: Array[Tree[T, U]] = new Array(numTrees)
  private val activeTree: mutable.BitSet = mutable.BitSet.empty

  override def getTree = tree

  override def isActive(treeId: Int): Boolean = activeTree.contains(treeId)

  override def merge(other: Forest[T, U]) : Unit = {
    var count = 0
    while (count < numTrees) {
      if (other.isActive(count)) {
        if (isActive(count)) {
          tree(count).merge(other.getTree(count))
        } else {
          tree(count) = other.getTree(count)
          activeTree += count
        }
      }
      count += 1
    }
  }

  /**
    * Add a tree to the forest
    *
    * @param treeId the tree index to add in the forest
    * @param tree_  the tree to add
    */
  def add(treeId: Int, tree_ : Tree[T, U]): Unit = {
    tree(treeId) = tree_
    activeTree += treeId
  }
}
