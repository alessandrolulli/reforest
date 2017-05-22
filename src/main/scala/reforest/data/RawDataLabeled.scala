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
  * The container for the raw data and the class label of an element
  *
  * @param label    the class label of the element
  * @param features the raw data of the element
  * @tparam T raw data type
  * @tparam U working data type
  */
case class RawDataLabeled[T, U](label: Double,
                                features: RawData[T, U]) extends Serializable {
  override def toString: String = {
    s"($label,$features)"
  }
}



