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

/**
  * It contains the information for the categorical features
  */
trait RFCategoryInfo extends Serializable {
  /**
    * How to re-map the value of a categorical feature
    *
    * @param featureValue the value of the feature
    * @return the new value for the feature
    */
  def rawRemapping(featureValue: Int): Int

  /**
    * It returns the arity of a categorical feature
    *
    * @param featureId the index of a feature
    * @return the arity of the feature
    */
  def getArity(featureId: Int): Int

  /**
    * To check if a feature is categorical
    *
    * @param featureId a feature index
    * @return true if the given feature index corresponds to a categorical feature
    */
  def isCategorical(featureId: Int): Boolean
}

/**
  * The base class to collect information about categorical features. To be used if the dataset contains categorical feature
  *
  * @param remappingValue the remapping map readed from file
  * @param arity          the arity configuration value readed from configuration file
  */
class RFCategoryInfoSpecialized(remappingValue: String, arity: String) extends RFCategoryInfo {
  private val arityMap = if (!arity.isEmpty) arity.split(",").map(t => t.split(":")).map(t => t(0).toInt -> t(1).toInt).toMap else Map[Int, Int]()
  private val allValue: Option[Int] = arityMap.get(-1)
  private val remappingMap = if (!remappingValue.isEmpty) remappingValue.split(",").map(t => t.split(":")).map(t => t(0).toInt -> t(1).toInt).toMap else Map[Int, Int]()

  override def rawRemapping(featureValue: Int): Int = {
    if (remappingMap.contains(featureValue))
      remappingMap(featureValue)
    else
      featureValue
  }

  override def getArity(featureId: Int): Int = {
    if (allValue.isDefined) {
      allValue.get
    }
    else {
      if (isCategorical(featureId))
        arityMap(featureId)
      else
        0
    }
  }

  override def isCategorical(featureId: Int): Boolean = {
    allValue.isDefined || arityMap.contains(featureId)
  }
}

/**
  * The base class to use if the dataset does NOT contains categorical feature
  */
class RFCategoryInfoEmpty() extends RFCategoryInfo {
  override def rawRemapping(featureValue: Int): Int = featureValue

  override def getArity(featureId: Int): Int = -1

  override def isCategorical(featureId: Int): Boolean = false
}
