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

import reforest.rf.RFCategoryInfo

/**
  * An utility to retrieve how must be large a data structure to contain the information relative to a given feature
  */
trait RFFeatureSizer extends Serializable {
  /**
    * It return the size requested by the given feature
    *
    * @param featureId a feature index
    * @return the minimum size required to store information for the feature (it considers the number of bins, the number of
    *         possible values for the feature, the missing values, the number of classes in the dataset)
    */
  def getSize(featureId: Int): Int

  /**
    * Starting from a bin number it returns the bin number when the number of bins is less that the number of bins stored in the static data
    *
    * @param featureId the feature identifier
    * @param value     the bin value
    * @return the shrinked bin value
    */
  def getShrinkedValue(featureId: Int, value: Int): Int

  /**
    * It is the reverse function of getShrinkedValue
    *
    * @param featureId the feature identifer
    * @param value     the bin value
    * @return the de-shrinked bin value
    */
  def getDeShrinkedValue(featureId: Int, value: Int): Int
}

/**
  * A feature sizer specialized for each feature
  *
  * @param splitNumberMap         a map containing the possible discretized values of each feature. Each value is <= number of configured bin
  * @param numClasses             the number of classes in the dataset
  * @param categoricalFeatureInfo the information about categorical features
  */
class RFFeatureSizerSpecialized(splitNumberMap: scala.collection.Map[Int, Int],
                                numClasses: Int,
                                categoricalFeatureInfo: RFCategoryInfo) extends RFFeatureSizer {

  override def getSize(featureId: Int): Int = {
    if (categoricalFeatureInfo.isCategorical(featureId))
      (categoricalFeatureInfo.getArity(featureId) + 1) * numClasses
    else
      (splitNumberMap(featureId) + 2) * numClasses
  }

  override def getShrinkedValue(featureId: Int, value: Int): Int = value

  override def getDeShrinkedValue(featureId: Int, value: Int): Int = value
}

class RFFeatureSizerSpecializedModelSelection(splitNumberMap: scala.collection.Map[Int, Int],
                                              numClasses: Int,
                                              categoricalFeatureInfo: RFCategoryInfo,
                                              binNumberShrinked: Int,
                                              binNumberMax: Int) extends RFFeatureSizer {

  override def getSize(featureId: Int): Int = {
    if (categoricalFeatureInfo.isCategorical(featureId))
      (categoricalFeatureInfo.getArity(featureId) + 1) * numClasses
    else {
      val splitNumberFeature = splitNumberMap(featureId)
      if (splitNumberFeature <= binNumberShrinked)
        (splitNumberFeature + 2) * numClasses
      else {
        (binNumberShrinked + 2) * numClasses
      }
    }
  }

  override def getShrinkedValue(featureId: Int, value: Int): Int = {
    if (binNumberShrinked >= binNumberMax) {
      value
    } else {
      if (value == 0) {
        value
      } else {
        val binnumberfeature = splitNumberMap(featureId) + 1
        if (binnumberfeature <= binNumberShrinked) {
          value
        } else {
          math.min(binNumberShrinked, math.max(1, (value / (binnumberfeature / binNumberShrinked.toDouble)).toInt))
        }
      }
    }
  }

  override def getDeShrinkedValue(featureId: Int, value: Int): Int = {
    if (binNumberShrinked >= binNumberMax) {
      value
    } else {
      val binNumberFeature = splitNumberMap(featureId) + 1
      if (binNumberFeature <= binNumberShrinked) {
        value
      } else {
        Math.min(binNumberFeature, value * (binNumberFeature / binNumberShrinked.toDouble).toInt)
      }
    }
  }
}

/**
  * A simple feature sizer that assign the size for each feature equal to the number of configured bin
  *
  * @param binNumber              the number of configured bin
  * @param numClasses             the number of classes in the dataset
  * @param categoricalFeatureInfo the information about categorical features
  */
class RFFeatureSizerSimple(binNumber: Int, numClasses: Int, categoricalFeatureInfo: RFCategoryInfo) extends RFFeatureSizer {

  override def getSize(featureId: Int): Int = {
    if (categoricalFeatureInfo.isCategorical(featureId))
      (categoricalFeatureInfo.getArity(featureId) + 1) * numClasses
    else
      (binNumber + 1) * numClasses
  }

  override def getShrinkedValue(featureId: Int, value: Int): Int = value

  override def getDeShrinkedValue(featureId: Int, value: Int): Int = value
}

class RFFeatureSizerSimpleModelSelection(binNumber: Int, numClasses: Int, categoricalFeatureInfo: RFCategoryInfo, binNumberShrinked: Int) extends RFFeatureSizer {

  override def getSize(featureId: Int): Int = {
    if (categoricalFeatureInfo.isCategorical(featureId))
      (categoricalFeatureInfo.getArity(featureId) + 1) * numClasses
    else {
      if (binNumber <= binNumberShrinked)
        (binNumber + 1) * numClasses
      else {
        (binNumberShrinked + 1) * numClasses
      }
    }
  }

  override def getShrinkedValue(featureId: Int, value: Int): Int = {
    if (binNumber <= binNumberShrinked || value == 0) {
      value
    } else {
      Math.min(binNumberShrinked, Math.max(1, (value / (binNumber / binNumberShrinked.toDouble)).toInt))
    }
  }

  override def getDeShrinkedValue(featureId: Int, value: Int): Int = {
    if (binNumber <= binNumberShrinked) {
      value
    } else {
      Math.min(binNumber, value * (binNumber / binNumberShrinked.toDouble).toInt)
    }
  }
}