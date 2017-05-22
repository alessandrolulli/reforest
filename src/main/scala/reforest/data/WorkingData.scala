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

/**
  * It contains the discretized data of the raw data
  * @tparam U working data type
  */
trait WorkingData[U] extends Serializable {

  /**
    * It returns the size of the data (i.e. the number of features)
    * @return The size if the data
    */
  def size: Int

  /**
    * It return the value at the given index
    * @param index The index for the return value
    * @return The value at the give index
    */
  def apply(index: Int): U
}

/**
  * Data saved in densed format. Each feature value is saved.
  * @param values The array for all the values of the element
  * @tparam U working data type
  */
class WorkingDataDense[U](val values: Array[U]) extends WorkingData[U] {

  override def size = values.length

  override def apply(index: Int) = values(index)
}

object WorkingDataDense {

}

/**
  * Data saved in sparse format. Only the known values are saved with a parallel structure where are saved the indices of the
  * known values.
  * @param size The size of the data (i.e. the number of features)
  * @param indices The known indicies of the element
  * @param values The values for the known indicies. Values in position i is relative to the feature having index equals to
  *               the value at position i in the indices structure
  * @param binIfNotFound the value to set if a feature value is not known
  * @tparam U working data type
  */
class WorkingDataSparse[U](override val size: Int,
                           val indices: Array[Int],
                           val values: Array[U],
                           val binIfNotFound: U) extends WorkingData[U] {
  override def apply(index: Int): U = {
    val indexInData = java.util.Arrays.binarySearch(indices, index)
    if (indexInData < 0) binIfNotFound else values(indexInData)
  }
}

object WorkingDataSparse {

}



