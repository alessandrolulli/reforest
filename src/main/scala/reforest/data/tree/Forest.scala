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
import reforest.data.{RawData, WorkingData}
import reforest.rf.feature.{RFFeatureManager, RFFeatureSizer, RFStrategyFeature}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * A base class to construct a forest
  *
  * @param numTrees       the total number of trees in the forest
  * @param maxDepth       the maximum allowed depth (from configuration)
  * @param binNumber      number of bin used to learn this forest
  * @param featurePerNode strategy to compute the number of features used in each node
  * @param featureSizer   the utility to correctly bin the values
  * @tparam T raw data type
  * @tparam U working data type
  */
abstract class Forest[T, U](val numTrees: Int,
                            val maxDepth: Int,
                            val binNumber: Int,
                            val featurePerNode: RFStrategyFeature,
                            val featureSizer: RFFeatureSizer) extends Serializable {

  /**
    * Update the forest with the newly passed cut
    *
    * @param cut      the cut to update the forest
    * @param forestId the forest identifier
    * @param treeId   the tree identifier
    * @param nodeId   the node identifier
    * @param typeInfo the type information for the raw data
    * @param typeInfoWorking the type information for the working data
    * @param featureManager the feature manager
    */
  def updateForest(cut: CutDetailed[T, U],
                   forestId: Int,
                   treeId: Int,
                   nodeId: Int,
                   typeInfo: TypeInfo[T],
                   typeInfoWorking: TypeInfo[U],
                   featureManager: Option[RFFeatureManager] = Option.empty): Unit = {

    if ((cut.left + cut.right) == 0) {
      if (cut.label.isDefined) {
        setLabel(treeId, nodeId, cut.label.get)
        setLeaf(treeId, nodeId)
      }
    } else if (cut.notValid > 0) {
      val cutNotValid = cut.getNotValid(typeInfo, typeInfoWorking)
      if (cut.label.isDefined) setLabel(treeId, nodeId, cut.label.get)

      if (cut.left > 0 && cut.right > 0) {
        setSplit(treeId, nodeId, cutNotValid)

        val leftChild = getLeftChild(treeId, nodeId)
        val rightChild = getRightChild(treeId, nodeId)
        setSplit(treeId, rightChild, cut)
        //              if (cut.label.isDefined) setLabel(treeId, rightChild, cut.label.get)

        val rightleftChild = getLeftChild(treeId, rightChild)
        val rightrightChild = getRightChild(treeId, rightChild)

        if (cut.labelNotValid.isDefined) {
          setLabel(treeId, leftChild, cut.labelNotValid.get)
        }
        if (cut.labelNotValidOk == cut.notValid || getLevel(treeId, leftChild) + 1 > maxDepth) {
          setLeaf(treeId, leftChild)
        } else {
          if (featureManager.isDefined) {
            featureManager.get.addFeatures(NodeId(forestId, treeId, leftChild), featurePerNode.getFeaturePerNode)
          }
        }

        if (cut.labelLeftOk == cut.left || getLevel(treeId, rightleftChild) + 1 > maxDepth) {
          setLeaf(treeId, rightleftChild)
          setLabel(treeId, rightleftChild, cut.labelLeft.get)
        } else {
          if (featureManager.isDefined) {
            featureManager.get.addFeatures(NodeId(forestId, treeId, rightleftChild), featurePerNode.getFeaturePerNode)
          }
        }

        if (cut.labelRightOk == cut.right || getLevel(treeId, rightrightChild) + 1 > maxDepth) {
          setLeaf(treeId, rightrightChild)
          setLabel(treeId, rightrightChild, cut.labelRight.get)
        } else {
          if (featureManager.isDefined) {
            featureManager.get.addFeatures(NodeId(forestId, treeId, rightrightChild), featurePerNode.getFeaturePerNode)
          }
        }
      } else {
        setSplit(treeId, nodeId, cutNotValid)
        if (cut.label.isDefined) setLabel(treeId, nodeId, cut.label.get)

        val leftChild = getLeftChild(treeId, nodeId)
        val rightChild = getRightChild(treeId, nodeId)
        if (cut.labelLeftOk > cut.labelRightOk) {
          setLabel(treeId, rightChild, cut.labelLeft.get)
        }
        else {
          setLabel(treeId, rightChild, cut.labelRight.get)
        }
        setLeaf(treeId, rightChild)

        if (cut.labelNotValid.isDefined) {
          setLabel(treeId, leftChild, cut.labelNotValid.get)
        }
        if (cut.labelNotValidOk == cut.notValid || getLevel(treeId, leftChild) + 1 > maxDepth) {
          setLeaf(treeId, leftChild)
        } else {
          if (featureManager.isDefined) {
            featureManager.get.addFeatures(NodeId(forestId, treeId, leftChild), featurePerNode.getFeaturePerNode)
          }
        }
      }
    } else {
      if (cut.left > 0 && cut.right > 0) {
        setSplit(treeId, nodeId, cut)
        if (cut.label.isDefined) setLabel(treeId, nodeId, cut.label.get)

        val leftChild = getLeftChild(treeId, nodeId)
        val rightChild = getRightChild(treeId, nodeId)

        if (cut.labelLeft.isDefined) {
          setLabel(treeId, leftChild, cut.labelLeft.get)
        }
        if (cut.labelRight.isDefined) {
          setLabel(treeId, rightChild, cut.labelRight.get)
        }

        if (cut.labelLeftOk == cut.left || getLevel(treeId, leftChild) + 1 > maxDepth) {
          setLeaf(treeId, leftChild)
        } else {
          if (featureManager.isDefined) {
            featureManager.get.addFeatures(NodeId(forestId, treeId, leftChild), featurePerNode.getFeaturePerNode)
          }
        }
        if (cut.labelRightOk == cut.right || getLevel(treeId, rightChild) + 1 > maxDepth) {
          setLeaf(treeId, rightChild)
        } else {
          if (featureManager.isDefined) {
            featureManager.get.addFeatures(NodeId(forestId, treeId, rightChild), featurePerNode.getFeaturePerNode)
          }
        }
      } else {
        setLabel(treeId, nodeId, cut.label.get)
        setLeaf(treeId, nodeId)
      }
    }
  }

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

  override def toString: String = {
    var toReturn = ""
    var treeId = 0
    while (treeId < numTrees) {
      if (isActive(treeId)) {
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
  def merge(other: Forest[T, U]): Unit

  /**
    * It predicts the class label of the given element
    *
    * @param maxDepth the maximum depth to visit
    * @param treeId   the index of the tree to use for prediction
    * @param data     the data to predict
    * @param typeInfo the type information of the war data
    * @return the predicted class label
    */
  def predict(maxDepth: Int, treeId: Int, data: RawData[T, U], typeInfo: TypeInfo[T]): Int = {
    getTree(treeId).predict(maxDepth, data, typeInfo)
  }

  /**
    * It returns the level of the given node id the the given tree
    *
    * @param treeId the tree index
    * @param nodeId the node index to check
    * @return the level in the tree treeId of the node nodeId
    */
  def getLevel(treeId: Int, nodeId: Int): Int = {
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
  def setLabel(treeId: Int, nodeId: Int, label: Int): Unit = {
    getTree(treeId).setLabel(nodeId, label)
  }

  /**
    * Set the split on the given node
    *
    * @param treeId the tree index
    * @param nodeId the node index for which setting the split
    * @param cut    the split to set in the node nodeId
    */
  def setSplit(treeId: Int, nodeId: Int, cut: CutDetailed[T, U]): Unit = {
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
    getTree(treeId).getCurrentNodeId(data, typeInfo, featureSizer)
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
    getTree(treeId).getCurrentNodeId(nodeId, data, typeInfo, featureSizer)
  }

  /**
    * It returns a list of nodes that still require to be processed
    *
    * @return a list of node that require computation
    */
  def getNodeToBeComputed: ListBuffer[(Int, Int)] = {
    val toReturn = new ListBuffer[(Int, Int)]()
    var treeId = 0
    while (treeId < numTrees) {
      toReturn ++= getTree(treeId).getNodeToBeComputed.map(nodeId => (treeId, nodeId))
      treeId += 1
    }
    toReturn
  }
}

/**
  * A forest where all the trees are initialized
  *
  * @param numTrees       the total number of trees in the forest
  * @param maxDepth       the maximum allowed depth (from configuration)
  * @param binNumber      number of bin used to learn this forest
  * @param featurePerNode strategy to compute the number of features used in each node
  * @param featureSizer   the utility to correctly bin the values
  * @tparam T raw data type
  * @tparam U working data type
  */
class ForestFull[T, U](override val numTrees: Int,
                       override val maxDepth: Int,
                       override val binNumber: Int,
                       override val featurePerNode: RFStrategyFeature,
                       override val featureSizer: RFFeatureSizer) extends Forest[T, U](numTrees, maxDepth, binNumber, featurePerNode, featureSizer) {
  private val tree: Array[Tree[T, U]] = Array.tabulate(numTrees)(_ => Tree.build[T, U](maxDepth))

  override def getTree: Array[Tree[T, U]] = tree

  override def isActive(treeId: Int): Boolean = true

  override def merge(other: Forest[T, U]): Unit = {
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
  * @param numTrees       the total number of trees in the forest
  * @param maxDepth       the maximum allowed depth (from configuration)
  * @param binNumber      number of bin used to learn this forest
  * @param featurePerNode strategy to compute the number of features used in each node
  * @param featureSizer   the utility to correctly bin the values
  * @tparam T raw data type
  * @tparam U working data type
  */
class ForestIncremental[T, U](override val numTrees: Int,
                              override val maxDepth: Int,
                              override val binNumber: Int,
                              override val featurePerNode: RFStrategyFeature,
                              override val featureSizer: RFFeatureSizer) extends Forest[T, U](numTrees, maxDepth, binNumber, featurePerNode, featureSizer) {
  private val tree: Array[Tree[T, U]] = new Array(numTrees)
  private val activeTree: mutable.BitSet = mutable.BitSet.empty

  def this(otherForest: Forest[T, U]) = this(otherForest.numTrees, otherForest.maxDepth, otherForest.binNumber, otherForest.featurePerNode, otherForest.featureSizer)

  override def getTree: Array[Tree[T, U]] = tree

  override def isActive(treeId: Int): Boolean = activeTree.contains(treeId)

  override def merge(other: Forest[T, U]): Unit = {
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
