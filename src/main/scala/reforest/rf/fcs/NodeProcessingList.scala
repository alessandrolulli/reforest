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

package reforest.rf.fcs

import scala.collection.mutable

trait NodeProcessingList[T] extends Serializable {
  def get() : T
  def add(element : T) : Unit
  def isEmpty : Boolean
}

class NodeProcessingQueue[T] extends NodeProcessingList[T] {
  val nodeQueue: mutable.Queue[T] = new mutable.Queue()

  def get() : T = nodeQueue.dequeue()
  def add(element : T) : Unit = nodeQueue.enqueue(element)
  def isEmpty : Boolean = nodeQueue.isEmpty
}

object NodeProcessingQueue {
  def create[T] = new NodeProcessingQueue[T]()
}

class NodeProcessingStack[T] extends NodeProcessingList[T] {
  val nodeStack: mutable.Stack[T] = new mutable.Stack()

  def get() : T = nodeStack.pop()
  def add(element : T) : Unit = nodeStack.push(element)
  def isEmpty : Boolean = nodeStack.isEmpty
}

object NodeProcessingStack {
  def create[T] = new NodeProcessingStack[T]()
}