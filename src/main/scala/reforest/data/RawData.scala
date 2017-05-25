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

package reforest.data

import reforest.rf.split.RFSplitter

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Forked from Apache Spark MLlib
  * https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/linalg/Vectors.scala
  *
  * It contains the working data stored in an optimized manner
  */
sealed trait RawData[T, U] extends Serializable {

  /**
    * To get the size of the values (i.e. the number of features)
    * @return the dimension of the values
    */
  def size: Int

  /**
    * A dense array representation of the values
    * @return the array representation of the values
    */
  def toArray: Array[T]

  /**
    * It returns the value for the feature i
    * @param i the index of the feature to return
    * @return the value for the feature i
    */
  def apply(i: Int): T

  /**
    * It applies the given function to all the active values
    * @param f the function to apply
    */
  def foreachActive(f: (Int, T) => Unit): Unit

  /**
    * The number of active features for this element
    * @return the number of active features
    */
  def numActives: Int

  /**
    * The number of not zero values for this element
    * @return the number of not zero values
    */
  def numNonzeros: Int

  /**
    * It returns a sparse representation of this element
    * @return a sparse representation of this element
    */
  def toSparse: RawDataSparse[T, U]

  /**
    * It returns a dense representation of this element
    * @return a dense representation of this element
    */
  def toDense: RawDataDense[T, U]

  /**
    * It returns a compressed representation of this element choosing between dense or sparse based on the number of active
    * values of this element
    * @return a sparse or dense representation. The one requiring less memory to store the element
    */
  def compressed: RawData[T, U] = {
    val nnz = numNonzeros
    if (1.5 * (nnz + 1.0) < size) {
      toSparse
    } else {
      toDense
    }
  }

  /**
    * It converts the raw data in working data dense format
    * @param splitter the manager that contains how to discretize the data
    * @return the dense discretized representation of this raw data
    */
  def toWorkingDataDense(splitter: RFSplitter[T, U]): WorkingDataDense[U]

  /**
    * It converts the raw data in working data sparse format
    * @param splitter the manager that contains how to discretize the data
    * @return the sparse discretized representation of this raw data
    */
  def toWorkingDataSparse(splitter: RFSplitter[T, U]): WorkingDataSparse[U]
}

object RawData {

  def dense[T: ClassTag, U: ClassTag](values: Array[T], nan: T): RawData[T, U] =
    new RawDataDense[T, U](values, nan)

  def sparse[T: ClassTag, U: ClassTag](size: Int, indices: Array[Int], values: Array[T], nan: T): RawData[T, U] =
    new RawDataSparse[T, U](size, indices, values, nan)

  def sparse[T: ClassTag, U: ClassTag](size: Int, elements: Seq[(Int, T)], nan: T): RawData[T, U] = {
    require(size > 0, "The size of the requested sparse vector must be greater than 0.")

    val (indices, values) = elements.sortBy(_._1).unzip
    var prev = -1
    indices.foreach { i =>
      require(prev < i, s"Found duplicate indices: $i.")
      prev = i
    }
    require(prev < size, s"You may not write an element to index $prev because the declared " +
      s"size of your vector is $size")

    new RawDataSparse[T, U](size, indices.toArray, values.toArray, nan)
  }

  val MAX_HASH_NNZ = 128
}

class RawDataDense[T: ClassTag, U: ClassTag](val values: Array[T],
                                             val nan: T) extends RawData[T, U] {

  override def toSparse: RawDataSparse[T, U] = {
    val nnz = numNonzeros
    val ii = new Array[Int](nnz)
    val vv = new Array[T](nnz)
    var k = 0
    foreachActive { (i, v) =>
      if (v != 0) {
        ii(k) = i
        vv(k) = v
        k += 1
      }
    }
    new RawDataSparse[T, U](size, ii, vv, nan)
  }

  override def size: Int = values.length

  override def toString: String = values.mkString("[", ",", "]")

  override def toArray: Array[T] = values

  override def apply(i: Int): T = values(i)

  override def foreachActive(f: (Int, T) => Unit): Unit = {
    var i = 0
    val localValuesSize = values.length
    val localValues = values

    while (i < localValuesSize) {
      f(i, localValues(i))
      i += 1
    }
  }

  override def numActives: Int = size

  override def numNonzeros: Int = {
    // same as values.count(_ != 0.0) but faster
    var nnz = 0
    values.foreach { v =>
      if (v != 0.0) {
        nnz += 1
      }
    }
    nnz
  }

  override def toDense = this

  def toWorkingDataDense(splitter: RFSplitter[T, U]): WorkingDataDense[U] = {
    var i = 0
    val arr = new Array[U](values.size)
    while (i < values.size) {
      arr(i) = splitter.getBin(i, values(i))
      i += 1
    }

    new WorkingDataDense(arr)
  }

  def toWorkingDataSparse(splitter: RFSplitter[T, U]): WorkingDataSparse[U] = {
    var i = 0
    val indices = new Array[Int](values.size)
    val arr = new Array[U](values.size)
    while (i < values.length) {
      indices(i) = i
      arr(i) = splitter.getBin(i, values(i))
      i += 1
    }

    new WorkingDataSparse(size, indices, arr, splitter.getBin(0, nan))
  }
}

object RawDataDense {


}

class RawDataSparse[T: ClassTag, U: ClassTag](
                                               override val size: Int,
                                               val indices: Array[Int],
                                               val values: Array[T],
                                               val nan: T) extends RawData[T, U] {


  override def toArray: Array[T] = {
    val data = new Array[T](size)
    var i = 0
    val nnz = indices.length
    while (i < nnz) {
      data(indices(i)) = values(i)
      i += 1
    }
    data
  }

  override def toDense: RawDataDense[T, U] = new RawDataDense[T, U](this.toArray, nan)

  override def apply(i: Int): T = {
    val idx = java.util.Arrays.binarySearch(indices, i)
    if (idx >= 0) values(idx)
    else nan
  }

  override def toString: String =
    s"($size,${indices.mkString("[", ",", "]")},${values.mkString("[", ",", "]")})"

  override def foreachActive(f: (Int, T) => Unit): Unit = {
    var i = 0
    val localValuesSize = values.length
    val localIndices = indices
    val localValues = values

    while (i < localValuesSize) {
      f(localIndices(i), localValues(i))
      i += 1
    }
  }

  override def numActives: Int = values.length

  override def numNonzeros: Int = {
    var nnz = 0
    values.foreach { v =>
      if (v != 0.0) {
        nnz += 1
      }
    }
    nnz
  }

  override def toSparse = this

  def toWorkingDataDense(splitter: RFSplitter[T, U]): WorkingDataDense[U] = {
    var i = 0
    val arr = new Array[U](size)
    val vv = toArray
    while (i < size) {
      arr(i) = splitter.getBin(i, vv(i))
      i += 1
    }

    new WorkingDataDense(arr)
  }

  def toWorkingDataSparse(splitter: RFSplitter[T, U]): WorkingDataSparse[U] = {
    var i = 0
    val arr = new Array[U](indices.size)
    while (i < indices.length) {
      arr(i) = splitter.getBin(indices(i), values(i))
      i += 1
    }

    new WorkingDataSparse(size, indices, arr, splitter.getBin(0, nan))
  }
}

object RawDataSparse {


}

