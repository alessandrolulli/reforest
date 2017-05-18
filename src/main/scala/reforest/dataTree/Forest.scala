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

abstract class Forest[T, U](val numTrees : Int, maxDepth : Int) extends Serializable {
  def getTree : Array[Tree[T, U]]
  def isActive(treeId : Int) : Boolean

  override def toString() = {
    var toReturn = ""
    var treeId = 0
    while(treeId < numTrees) {
      toReturn += "TREE ID: "+treeId+"\n"+getTree(treeId).toString+"\n\n"
      treeId += 1
    }
    toReturn
  }

  def merge(other : Forest[T, U]) = {
    var count = 0
    while(count < numTrees) {
      if(other.isActive(count)) {
        if(isActive(count)) {
          getTree(count).merge(other.getTree(count))
        } else {
          getTree(count) = other.getTree(count)
        }
      }
      count += 1
    }
  }

  def predict(treeId : Int, data: RawData[T, U], typeInfo: TypeInfo[T]) = {
    getTree(treeId).predict(data, typeInfo)
  }

  def getLevel(treeId : Int, nodeId : Int) = {
    getTree(treeId).getLevel(nodeId)
  }

  def getLeftChild(treeId : Int, nodeId : Int) : Int = {
    getTree(treeId).getLeftChild(nodeId)
  }

  def getRightChild(treeId : Int, nodeId : Int) : Int = {
    getTree(treeId).getRightChild(nodeId)
  }

  def getParent(treeId : Int, nodeId : Int) : Int = {
    getTree(treeId).getParent(nodeId)
  }

  def setLabel(treeId : Int, nodeId : Int, label : Int) = {
    getTree(treeId).setLabel(nodeId, label)
  }

  def setSplit(treeId : Int, nodeId : Int, cut : CutDetailed[T, U]) = {
    getTree(treeId).setSplit(nodeId, cut)
  }

  def setLeaf(treeId : Int, nodeId : Int) = {
    getTree(treeId).setLeaf(nodeId)
  }

  def isLeaf(treeId : Int, nodeId : Int) : Boolean = {
    getTree(treeId).isLeaf(nodeId)
  }

  def isDefined(treeId : Int, nodeId : Int) : Boolean = {
    getTree(treeId).isDefined(nodeId)
  }

  def getCurrentNodeId(treeId : Int, data: WorkingData[U], typeInfo: TypeInfo[U]): Option[Int] = {
    getTree(treeId).getCurrentNodeId(data, typeInfo)
  }

  def getCurrentNodeId(treeId : Int, nodeId : Int, data: WorkingData[U], typeInfo: TypeInfo[U]): Option[Int] = {
    getTree(treeId).getCurrentNodeId(nodeId, data, typeInfo)
  }

  def getNodeToBeComputed() : ListBuffer[(Int, Int)] = {
    val toReturn = new ListBuffer[(Int, Int)]()
    var treeId = 0
    while(treeId < numTrees) {
      toReturn ++= getTree(treeId).getNodeToBeComputed().map(nodeId => (treeId, nodeId))
      treeId += 1
    }
    toReturn
  }
}

class ForestFull[T, U](numTrees : Int, maxDepth : Int) extends Forest[T, U](numTrees, maxDepth) {
  val tree = Array.tabulate(numTrees)(_ => new Tree[T, U](maxDepth))

  override def getTree = tree
  override def isActive(treeId : Int) : Boolean = true
}

class ForestIncremental[T, U](numTrees : Int, maxDepth : Int) extends Forest[T, U](numTrees, maxDepth) {
  val tree : Array[Tree[T, U]] = new Array(numTrees)
  val activeTree : mutable.BitSet = mutable.BitSet.empty

  override def getTree = tree
  override def isActive(treeId : Int) : Boolean = activeTree.contains(treeId)

  def add(treeId : Int, tree_ : Tree[T, U]) = {
    tree(treeId) = tree_
    activeTree += treeId
  }
}
