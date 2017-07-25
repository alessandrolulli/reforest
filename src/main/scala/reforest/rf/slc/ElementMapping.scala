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

import java.util

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Data structure to maintain the indices of the elements in the dataset amenable for a specific node
  */
trait ElementMapping extends Serializable {
  /**
    * Check if the data structure is empty
    *
    * @return true if the data structure is empty
    */
  def isEmpty: Boolean

  /**
    * Insert an index in the data structure
    *
    * @param element the index of the element to be inserted
    */
  def add(element: Int): Unit

  /**
    * Retrieve all the indices contained in the data structure
    *
    * @return an iterable of indices
    */
  def getMapping: Iterable[Int]

  def toStore : Option[ElementMapping]
}

/**
  * Utility object to create an Element mapping
  */
object ElementMapping {
  /**
    * Utility to create an Element mapping based on the current depth processed and the number of sample in the dataset
    *
    * @param sampleSize the number of elements in the dataset
    * @param depth      the currently processed depth
    * @param maxDepth   the maximum depth to be processed
    * @return an optimized implementation of the ElementMapping class
    */
  def build(sampleSize: Long, depth: Int, maxDepth: Int): ElementMapping = {
    if ((depth + 1) >= maxDepth) {
      new ElementMappingEmpty
    } else if (sampleSize < ((sampleSize / Math.pow(2, depth)) * (32 + 32))) {
//      new ElementMappingEmpty
//      new ElementMappingArrayBuffer(((sampleSize / Math.pow(2, depth)).toInt))
//      new ElementMappingListBuffer
      new ElementMappingBitSet
    } else {
      new ElementMappingArrayBuffer(((sampleSize / Math.pow(2, depth)).toInt))
//      new ElementMappingEmpty
    }
  }
}

/**
  * A BitSet implementation of the ElementMapping data structure
  */
class ElementMappingBitSet extends ElementMapping {
  private val mapping: mutable.BitSet = mutable.BitSet.empty

  override def isEmpty = mapping.isEmpty

  override def add(element: Int) = mapping += element

  override def getMapping() = mapping

  override def toStore() = Some(this)
}

/**
  * A Java BitSet implementation of the ElementMapping data structure
  */
class ElementMappingBitSetJava extends ElementMapping {
  private val mapping: java.util.BitSet = new util.BitSet()

  /**
    * It provides a functionality to convert a util.BitSet in a Scala iterable
    * @param m
    */
  class ElementMappingBitSetJavaIterable(m: util.BitSet) extends Iterable[Int] {
    /**
      * It generates an iterator of the provided util.BitSet
      * @return an iterator of the indices
      */
    def iterator: Iterator[Int] = {
      class ElementMappingBitSetJavaIterator extends Iterator[Int] {

        var i = m.nextSetBit(0)

        override def next: Int = {
          val oldI = i
          i = m.nextSetBit(i + 1)

          oldI
        }

        def hasNext: Boolean = {
          i != -1
        }
      }

      new ElementMappingBitSetJavaIterator()
    }
  }

  override def isEmpty = mapping.isEmpty

  override def add(element: Int) = mapping.set(element)

  override def getMapping() = new ElementMappingBitSetJavaIterable(mapping)

  override def toStore() = Some(this)
}

/**
  * A ListBuffer implementation of the ElementMapping data structure
  */
class ElementMappingListBuffer extends ElementMapping {
  private val mapping: ListBuffer[Int] = ListBuffer.empty

  override def isEmpty = mapping.isEmpty

  override def add(element: Int) = mapping += element

  override def getMapping() = mapping

  override def toStore() = Some(this)
}

/**
  * An ArrayBuffer implementation of the ElementMapping data structure
  */
class ElementMappingArrayBuffer(initialSize : Int) extends ElementMapping {
  val mapping: ArrayBuffer[Int] = new ArrayBuffer[Int](initialSize)

  override def isEmpty = mapping.isEmpty

  override def add(element: Int) = mapping += element

  override def getMapping() = mapping

  override def toStore() = Some(this)
}

/**
  * An Empty implementation of the ElementMapping data structure.
  * This is used when we do not need to remember the indices.  This implementation permits to have the code identical
  * altough not requiring memory consumption.
  * This is particularly useful when processing node that are at the maximum computable depth in order to not
  * remember the mapping for its children that will not be computed.
  */
class ElementMappingEmpty extends ElementMapping {
  override def isEmpty = true

  override def add(element: Int) = {}

  override def getMapping() = Iterable.empty

  override def toStore() = Option.empty
}
