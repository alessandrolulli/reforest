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

/**
  * Forked from Apache Spark MLlib
  * https://github.com/apache/spark/blob/39e2bad6a866d27c3ca594d15e574a1da3ee84cc/mllib/src/main/scala/org/apache/spark/ml/tree/Node.scala
  */
class TreeNode[T, U](var id: Int,
                     var label: Option[Int],
                     var leftChild: Option[TreeNode[T, U]],
                     var rightChild: Option[TreeNode[T, U]],
                     var split: Option[Cut[T, U]],
                     var isLeaf: Boolean) extends Serializable {

  def predict(data: RawDataLabeled[T, U], typeInfo: TypeInfo[T]): Double = {
    predict(data.features, typeInfo)
  }

  def predict(data: RawData[T, U], typeInfo: TypeInfo[T]): Int = {
    if (this.isLeaf || this.split.isEmpty) {
      this.label.getOrElse(-1)
    } else {
      val split = this.split.get
      val splitLeft = split.shouldGoLeft(data, typeInfo)

      if (splitLeft) {
        this.leftChild.get.predict(data, typeInfo)
      } else {
        this.rightChild.get.predict(data, typeInfo)
      }
    }
  }

  def getCurrentNodeId(data: WorkingData[U], typeInfo: TypeInfo[U]): Option[Int] = {
    if (this.isLeaf) Option.empty
    else if (this.split.isEmpty) {
      Some(this.id)
    } else {
      val splitLeft = this.split.get.shouldGoLeftBin(data, typeInfo)

      if (splitLeft) {
        this.leftChild.get.getCurrentNodeId(data, typeInfo)
      } else {
        this.rightChild.get.getCurrentNodeId(data, typeInfo)
      }

    }
  }

  def getLeftChild(): TreeNode[T, U] = {
    if (leftChild.isEmpty) leftChild = Some(TreeNode.emptyNode(TreeNode.leftChildIndex(id)))
    leftChild.get
  }

  def isLeftChildLeaf(maxDepth: Int) = {
    getLeftChild().isLeaf || TreeNode.indexToLevelCheck(getLeftChild().id) >= maxDepth
  }

  def getRightChild(): TreeNode[T, U] = {
    if (rightChild.isEmpty) rightChild = Some(TreeNode.emptyNode(TreeNode.rightChildIndex(id)))
    rightChild.get
  }

  def isRightChildLeaf(maxDepth: Int) = {
    getRightChild().isLeaf || TreeNode.indexToLevelCheck(getRightChild().id) >= maxDepth
  }

  def setSplit(splitToSet: Cut[T, U]) = {
    split = Some(splitToSet)
    (getLeftChild(), getRightChild())
  }

  def close(labelToSet: Int) = {
    isLeaf = true
    label = Some(labelToSet)
  }

  override def toString: String = {
    val space = Array.fill[String](TreeNode.indexToLevel(id))("\t").mkString("")
    val toPrint = Array(id.toString, label.toString, split.toString, isLeaf.toString)
    var left = ""
    if (leftChild.isDefined) left = "\n" + leftChild.get.toString
    var right = ""
    if (rightChild.isDefined) right = "\n" + rightChild.get.toString
    space + toPrint.mkString(",") + left + right
  }

}

object TreeNode {

  /** Create an empty node with the given node index.  Values must be set later on. */
  def emptyNode[T, U](nodeIndex: Int): TreeNode[T, U] = {
    new TreeNode(nodeIndex, None, None, None, None, false)
  }

  // The below indexing methods were copied from spark.mllib.tree.model.Node

  /**
    * Return the index of the left child of this node.
    */
  def leftChildIndex(nodeIndex: Int): Int = nodeIndex << 1

  /**
    * Return the index of the right child of this node.
    */
  def rightChildIndex(nodeIndex: Int): Int = (nodeIndex << 1) + 1

  /**
    * Get the parent index of the given node, or 0 if it is the root.
    */
  def parentIndex(nodeIndex: Int): Int = nodeIndex >> 1

  /**
    * Return the level of a tree which the given node is in.
    */
  def indexToLevel(nodeIndex: Int): Int = if (nodeIndex == 0) {
    throw new IllegalArgumentException(s"0 is not a valid node index.")
  } else {
    java.lang.Integer.numberOfTrailingZeros(java.lang.Integer.highestOneBit(nodeIndex))
  }

  def indexToLevelCheck(nodeIndex: Int): Int = if (nodeIndex == 0) {
    throw new IllegalArgumentException(s"0 is not a valid node index.")
  } else {
    java.lang.Integer.numberOfTrailingZeros(java.lang.Integer.highestOneBit(nodeIndex)) + 1
  }

  /**
    * Returns true if this is a left child.
    * Note: Returns false for the root.
    */
  def isLeftChild(nodeIndex: Int): Boolean = nodeIndex > 1 && nodeIndex % 2 == 0

  /**
    * Return the maximum number of nodes which can be in the given level of the tree.
    *
    * @param level Level of tree (0 = root).
    */
  def maxNodesInLevel(level: Int): Int = 1 << level

  /**
    * Return the index of the first node in the given level.
    *
    * @param level Level of tree (0 = root).
    */
  def startIndexInLevel(level: Int): Int = 1 << level

  /**
    * Traces down from a root node to get the node with the given node index.
    * This assumes the node exists.
    */
  def getNode[T, U](nodeIndex: Int, rootNode: TreeNode[T, U]): TreeNode[T, U] = {
    var tmpNode: TreeNode[T, U] = rootNode
    var levelsToGo = indexToLevel(nodeIndex)
    while (levelsToGo > 0) {
      if ((nodeIndex & (1 << levelsToGo - 1)) == 0) {
        tmpNode = tmpNode.leftChild.get
      } else {
        tmpNode = tmpNode.rightChild.get
      }
      levelsToGo -= 1
    }
    tmpNode
  }

}
