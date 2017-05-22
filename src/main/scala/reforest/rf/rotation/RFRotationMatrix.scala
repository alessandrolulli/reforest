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

package reforest.rf.rotation

import org.apache.spark.broadcast.Broadcast
import reforest.TypeInfo
import reforest.data.{RawData, RawDataDense, RawDataLabeled, RotationMatrix}

import scala.reflect.ClassTag

/**
  * To rotate the raw data
  *
  * @param n        the size of the nxn matrix (typically n is the number of features in the dataset)
  * @param typeInfo the type information for the raw data
  * @param seed     a random generator seed
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFRotationMatrix[T: ClassTag, U: ClassTag](n: Int, typeInfo: Broadcast[TypeInfo[T]], seed: Int) extends Serializable {

  private val matrix = new RotationMatrix(n, seed)

  /**
    * It rotates a raw data
    *
    * @param element the element to rotate
    * @return the rotated element
    */
  def rotateRawData(element: RawData[T, U]) = {
    val dense = element.toDense
    val densedRotated = matrix.rotate(dense.values, typeInfo.value)

    new RawDataDense[T, U](densedRotated, dense.nan)
  }

  /**
    * It rotates a raw data labeled
    *
    * @param element the element to rotate
    * @return the rotated element
    */
  def rotate(element: RawDataLabeled[T, U]) = {
    new RawDataLabeled[T, U](element.label, rotateRawData(element.features))
  }
}
