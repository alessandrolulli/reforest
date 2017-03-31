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

trait WorkingData[U] extends Serializable {

  def size: Int
  def apply(index: Int): U
}

class WorkingDataDense[U](val values: Array[U]) extends WorkingData[U] {

  override def size = values.length

  override def apply(index: Int) = values(index)
}

object WorkingDataDense {

}

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



