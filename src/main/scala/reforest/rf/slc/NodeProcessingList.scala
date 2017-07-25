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

package reforest.rf.slc

import scala.collection.mutable

/**
  * Data structure containing the list of nodes that still need the computation in SLC.
  * In general it is an interface representing a list of objects that has implementation as a Queue or as a Stack
  * @tparam T the data type contained in the data structure
  */
trait NodeProcessingList[T] extends Serializable {
  /**
    * Return the element on top of the data structure
    * @return the top element in the data structure
    */
  def get() : T

  /**
    * Insert an element in the data structure
    * @param element the element to be inserted
    */
  def add(element : T) : Unit

  /**
    * Check if the data structure is empty
    * @return true if the data structure is empty
    */
  def isEmpty : Boolean

  def clear : Unit
}

/**
  * A Queue implementation of the NodeProcessingList
  * @tparam T the data type contained in the data structure
  */
class NodeProcessingQueue[T] extends NodeProcessingList[T] {
  private val nodeQueue: mutable.Queue[T] = new mutable.Queue()

  def get() : T = nodeQueue.dequeue()
  def add(element : T) : Unit = nodeQueue.enqueue(element)
  def isEmpty : Boolean = nodeQueue.isEmpty
  def clear : Unit = nodeQueue.clear()
}

/**
  * Utility to create a NodeProcessingQueue
  */
object NodeProcessingQueue {
  def create[T] = new NodeProcessingQueue[T]()
}

/**
  * A Stack implementation of the NodeProcessingList
  * @tparam T the data type contained in the data structure
  */
class NodeProcessingStack[T] extends NodeProcessingList[T] {
  private val nodeStack: mutable.Stack[T] = new mutable.Stack()

  def get() : T = nodeStack.pop()
  def add(element : T) : Unit = nodeStack.push(element)
  def isEmpty : Boolean = nodeStack.isEmpty
  def clear : Unit = nodeStack.clear()
}

/**
  * Utility to create a NodeProcessingStack
  */
object NodeProcessingStack {
  def create[T] = new NodeProcessingStack[T]()
}