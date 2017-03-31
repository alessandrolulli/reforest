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

package reforest.rf

trait RFCategoryInfo extends Serializable {
  def rawRemapping(featureValue: Int): Int

  def getArity(featureId: Int): Int

  def isCategorical(featureId: Int): Boolean
}

class RFCategoryInfoSpecialized(remappingValue: String, arity: String) extends RFCategoryInfo {
  val arityMap = if(!arity.isEmpty) arity.split(",").map(t => t.split(":")).map(t => t(0).toInt -> t(1).toInt).toMap else Map[Int, Int]()
  val allValue: Option[Int] = arityMap.get(-1)
  val remappingMap = if(!remappingValue.isEmpty) remappingValue.split(",").map(t => t.split(":")).map(t => t(0).toInt -> t(1).toInt).toMap else Map[Int, Int]()

  override def rawRemapping(featureValue: Int): Int = {
    if(remappingMap.contains(featureValue))
      remappingMap(featureValue)
    else
      featureValue
  }

  override def getArity(featureId: Int): Int = {
    if (allValue.isDefined) {
      allValue.get
    }
    else {
      arityMap(featureId)
    }
  }

  override def isCategorical(featureId: Int): Boolean = {
    allValue.isDefined || arityMap.contains(featureId)
  }
}

class RFCategoryInfoEmpty() extends RFCategoryInfo {
  override def rawRemapping(featureValue: Int): Int = featureValue
  override def getArity(featureId: Int): Int = -1
  override def isCategorical(featureId: Int): Boolean = false
}
