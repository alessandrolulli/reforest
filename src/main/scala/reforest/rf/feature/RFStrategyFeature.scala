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

package reforest.rf.feature

/**
  * It characterize the number of features to use as candidates for splitting at each tree node.
  */
trait RFStrategyFeature extends Serializable {
  /**
    * A string description of the class
    * @return the description
    */
  def getDescription: String

  /**
    * The number of features in the dataset
    * @return the number of features in the dataset
    */
  def getFeatureNumber : Int

  /**
    * A new sample of feature indices to use on a node
    * @return an array with the feature indices
    */
  def getFeaturePerNode : Array[Int]

  /**
    * The number of features to use as candidates for splitting at each tree node
    * @return the number of features for splitting at each tree node
    */
  def getFeaturePerNodeNumber : Int
}

/**
  * It characterize the number of features to use as candidates for splitting at each tree node.
  * It uses ALL the features
  * @param featureNumber
  */
class RFStrategyFeatureALL(featureNumber : Int) extends RFStrategyFeature {
  val featurePerNode = Array.tabulate(featureNumber)(i => i)

  def getDescription() : String = "ALL"
  def getFeatureNumber : Int = featureNumber
  def getFeaturePerNode : Array[Int] = featurePerNode
  def getFeaturePerNodeNumber : Int = featureNumber
}

/**
  * It characterize the number of features to use as candidates for splitting at each tree node.
  * It is a base class for returning a subset of the features
  * @param featureNumber the number of features in the dataset
  */
abstract class RFStrategyFeatureBase(featureNumber : Int) extends RFStrategyFeature {
  private val r = scala.util.Random

  def getFeatureNumber : Int = featureNumber
  def getFeaturePerNode : Array[Int] = {
    var toReturn = Set[Int]()
    while (toReturn.size < getFeaturePerNodeNumber) {
      toReturn = toReturn + r.nextInt(getFeatureNumber)
    }
    toReturn.toArray
  }
}

/**
  * It characterize the number of features to use as candidates for splitting at each tree node.
  * It uses a CUSTOM number of the features
  * @param featureNumber
  */
class RFStrategyFeatureCUSTOM(featureNumber : Int, featurePerNode : Int) extends RFStrategyFeatureBase(featureNumber) {
  override def getDescription() : String = "CUSTOM-"+featurePerNode
  override def getFeaturePerNodeNumber : Int = featurePerNode
}

/**
  * It characterize the number of features to use as candidates for splitting at each tree node.
  * It uses SQRT features
  * @param featureNumber the number of features in the dataset
  */
class RFStrategyFeatureSQRT(featureNumber : Int) extends RFStrategyFeatureBase(featureNumber) {
  override def getDescription() : String = "SQRT"
  override def getFeaturePerNodeNumber : Int = Math.sqrt(featureNumber).ceil.toInt
}

/**
  * It characterize the number of features to use as candidates for splitting at each tree node.
  * It uses SQRTSQRT features
  * @param featureNumber the number of features in the dataset
  */
class RFStrategyFeatureSQRTSQRT(featureNumber : Int) extends RFStrategyFeatureBase(featureNumber) {
  override def getDescription() : String = "SQRTSQRT"
  override def getFeaturePerNodeNumber : Int = Math.sqrt(Math.sqrt(featureNumber)).ceil.toInt
}

/**
  * It characterize the number of features to use as candidates for splitting at each tree node.
  * It uses LOG2 features
  * @param featureNumber the number of features in the dataset
  */
class RFStrategyFeatureLOG2(featureNumber : Int) extends RFStrategyFeatureBase(featureNumber) {
  override def getDescription() : String = "LOG2"
  override def getFeaturePerNodeNumber : Int = (scala.math.log(featureNumber) / scala.math.log(2)).ceil.toInt
}

/**
  * It characterize the number of features to use as candidates for splitting at each tree node.
  * It uses ONETHIRD features
  * @param featureNumber the number of features in the dataset
  */
class RFStrategyFeatureONETHIRD(featureNumber : Int) extends RFStrategyFeatureBase(featureNumber) {
  override def getDescription() : String = "ONETHIRD"
  override def getFeaturePerNodeNumber : Int = (featureNumber.toDouble / 3).ceil.toInt
}