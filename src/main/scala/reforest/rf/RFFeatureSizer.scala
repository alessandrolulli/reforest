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
}

/**
  * A feature sizer specialized for each feature
  *
  * @param binNumberMap           a map containing the possible discretized values of each feature. Each value is <= number of configured bin
  * @param numClasses             the number of classes in the dataset
  * @param categoricalFeatureInfo the information about categorical features
  */
class RFFeatureSizerSpecialized(binNumberMap: scala.collection.Map[Int, Int],
                                numClasses: Int,
                                categoricalFeatureInfo: RFCategoryInfo) extends RFFeatureSizer {

  override def getSize(featureId: Int) = {
    if (categoricalFeatureInfo.isCategorical(featureId))
      (categoricalFeatureInfo.getArity(featureId) + 1) * numClasses
    else
      (binNumberMap(featureId) + 2) * numClasses
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

  override def getSize(featureId: Int) = {
    if (categoricalFeatureInfo.isCategorical(featureId))
      (categoricalFeatureInfo.getArity(featureId) + 1) * numClasses
    else
      (binNumber + 1) * numClasses
  }
}