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

import java.util

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

trait ElementMapping extends Serializable {
  def isEmpty : Boolean
  def add(element : Int) : Unit
  def getMapping() : Iterable[Int]
}

object ElementMapping {
  def build(sampleSize : Long, depth : Int, maxDepth : Int) : ElementMapping = {
    if((depth+1) >= maxDepth) {
      new ElementMappingEmpty
    } else if(sampleSize < ((sampleSize / Math.pow(2, depth))*(32 + 32))) {
      new ElementMappingBitSet
    } else {
      new ElementMappingArrayBuffer
    }
  }
}

class ElementMappingBitSet extends ElementMapping {
  val mapping : mutable.BitSet = mutable.BitSet.empty

  override def isEmpty = mapping.isEmpty
  override def add(element : Int) = mapping += element
  override def getMapping() = mapping
}

class ElementMappingBitSetJava extends ElementMapping {
  val mapping : java.util.BitSet = new util.BitSet()

  class ElementMappingBitSetJavaIterable(m : util.BitSet) extends Iterable[Int] {
    def iterator: Iterator[Int] = {
      class ElementMappingBitSetJavaIterator extends Iterator[Int] {

        var i = m.nextSetBit(0)

        override def next : Int = {
          val oldI = i
          i = m.nextSetBit(i + 1)

          oldI
        }

        def hasNext : Boolean = {
          i != -1
        }
      }

      new ElementMappingBitSetJavaIterator()
    }
  }

  override def isEmpty = mapping.isEmpty
  override def add(element : Int) = mapping.set(element)
  override def getMapping() = new ElementMappingBitSetJavaIterable(mapping)
}

class ElementMappingListBuffer extends ElementMapping {
  val mapping : ListBuffer[Int] = ListBuffer.empty

  override def isEmpty = mapping.isEmpty
  override def add(element : Int) = mapping += element
  override def getMapping() = mapping
}

class ElementMappingArrayBuffer extends ElementMapping {
  val mapping : ArrayBuffer[Int] = ArrayBuffer.empty

  override def isEmpty = mapping.isEmpty
  override def add(element : Int) = mapping += element
  override def getMapping() = mapping
}

class ElementMappingEmpty extends ElementMapping {
  override def isEmpty = true
  override def add(element : Int) = {}
  override def getMapping() = Iterable.empty
}
