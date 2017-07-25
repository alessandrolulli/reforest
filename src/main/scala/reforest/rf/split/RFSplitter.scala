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

package reforest.rf.split

import reforest.TypeInfo
import reforest.rf._
import reforest.rf.feature._

import scala.collection.Map
import scala.util.Random
import collection.JavaConverters._

/**
  * It contains the split to discretize the raw data in working data
  *
  * @tparam T raw data type
  * @tparam U working data type
  */
trait RFSplitter[T, U] extends Serializable {

  /**
    * The number of bin for the given feature
    *
    * @param idFeature the feature identifier
    * @return the number of bin for the given feature
    */
  def getBinNumber(idFeature: Int): U

  /**
    * Discretize a value to the correct bin number
    *
    * @param idFeature the feature identifier
    * @param value     the raw value
    * @return the discretized value
    */
  def getBin(idFeature: Int, value: T): U

  /**
    * Retrieve the raw value from a bin number
    *
    * @param idFeature the feature identifier
    * @param cut       the bin number of the cut
    * @return the raw value
    */
  def getRealCut(idFeature: Int, cut: U): T

  /**
    * It generates a RFFeatureSizer to know how large a data structure must be to contain all the contributions
    *
    * @param numClasses the number of classes in the dataset
    * @return a specialized RFFeatureSizer
    */
  def generateRFSizer(numClasses: Int): RFFeatureSizer

  /**
    * It generates a RFFeatureSizer to know how large a data structure must be to contain all the contributions.
    * This must be used during the model selection when requesting a RFFeatureSizer for less number of bin
    * with respect to the number of bins in the working data
    *
    * @param numClasses        the number of classes in the dataset
    * @param binNumberShrinked the maximum number of bin used
    * @return
    */
  def generateRFSizer(numClasses: Int, binNumberShrinked: Int): RFFeatureSizer
}

/**
  * The implementation of RFSplitter when each feature may have a different number of bin (until a max configured value)
  *
  * @param split                  a map which contains the splits for each feature
  * @param typeInfo               the type information for the raw data
  * @param typeInfoWorking        the type information for the working data
  * @param categoricalFeatureInfo the information about the categorical features
  * @param binNumber              the number of bin in the working data
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFSplitterSpecialized[T, U](split: Map[Int, Array[T]],
                                  typeInfo: TypeInfo[T],
                                  typeInfoWorking: TypeInfo[U],
                                  categoricalFeatureInfo: RFCategoryInfo,
                                  binNumber: Int) extends RFSplitter[T, U] {

  def this(split: java.util.HashMap[Int, Array[T]],
           typeInfo: TypeInfo[T],
           typeInfoWorking: TypeInfo[U],
           categoricalFeatureInfo: RFCategoryInfo,
           binNumber: Int) = this(split.asScala, typeInfo, typeInfoWorking, categoricalFeatureInfo, binNumber)

  override def getBinNumber(idFeature: Int): U = {
    if (categoricalFeatureInfo.isCategorical(idFeature))
      typeInfoWorking.fromInt(categoricalFeatureInfo.getArity(idFeature))
    else
      typeInfoWorking.fromInt(split(idFeature).length + 2)
  }

  override def getBin(index: Int, value: T): U = {
    if (typeInfo.isNOTvalidDefined && !typeInfo.isValidForBIN(value)) {
      typeInfoWorking.zero
    } else {
      if (categoricalFeatureInfo.isCategorical(index)) {
        typeInfoWorking.fromInt(typeInfo.toInt(value) + 1)
      } else {
        val split2 = split(index)

        val idx = typeInfo.getIndex(split2, value)
        val idx2 = -idx - 1
        typeInfoWorking.fromInt((Math.min(Math.max(idx, idx2), split2.length) + 1))
      }
    }
  }

  override def getRealCut(index: Int, cut: U): T = {
    typeInfo.getRealCut(typeInfoWorking.toInt(cut), split(index))
  }

  override def generateRFSizer(numClasses: Int): RFFeatureSizer = {
    new RFFeatureSizerSpecialized(split.map(t => (t._1, t._2.length)), numClasses, categoricalFeatureInfo)
  }

  override def generateRFSizer(numClasses: Int, binNumberShrinked: Int): RFFeatureSizer = {
    if (binNumberShrinked != binNumber) {
      new RFFeatureSizerSpecializedModelSelection(split.map(t => (t._1, t._2.length)), numClasses, categoricalFeatureInfo, binNumberShrinked, binNumber)
    } else {
      generateRFSizer(numClasses)
    }
  }
}

/**
  * The implementation of RFSplitter when the same splits are used for all the features
  *
  * @param minT                   the minimum value in the dataset
  * @param maxT                   the maximum value in the dataset
  * @param typeInfo               the type information for the raw data
  * @param typeInfoWorking        the type information for the working data
  * @param numberBin              the number of bin in the working data
  * @param categoricalFeatureInfo the information about the categorical features
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFSplitterSimpleRandom[T, U](minT: T,
                                   maxT: T,
                                   typeInfo: TypeInfo[T],
                                   typeInfoWorking: TypeInfo[U],
                                   numberBin: Int,
                                   categoricalFeatureInfo: RFCategoryInfo) extends RFSplitter[T, U] {

  private val min = typeInfo.toDouble(minT)
  private val max = typeInfo.toDouble(maxT)
  private val range = max - min

  private val randomSplit = Array.fill(numberBin - 1)(Random.nextDouble()).toList.sorted.toArray
  private val splitValue = randomSplit.map(t => (range * t) + min)

  override def getBinNumber(idFeature: Int): U = {
    typeInfoWorking.fromInt(numberBin + 1)
  }

  override def getBin(index: Int, value: T): U = {
    val idx = java.util.Arrays.binarySearch(splitValue, typeInfo.toDouble(value))
    val idx2 = -idx - 1
    typeInfoWorking.fromInt((Math.min(Math.max(idx, idx2), splitValue.length) + 1))
  }

  override def getRealCut(index: Int, cut: U): T = {
    val cutDouble = typeInfoWorking.toInt(cut)
    if (cutDouble < 0)
      typeInfo.fromDouble(0d)
    else if ((cutDouble - 1) >= splitValue.length)
      typeInfo.maxValue
    else {
      typeInfo.fromDouble(splitValue(cutDouble - 1))
    }
  }

  override def generateRFSizer(numClasses: Int): RFFeatureSizer = {
    new RFFeatureSizerSimple(numberBin, numClasses, categoricalFeatureInfo)
  }

  override def generateRFSizer(numClasses: Int, binNumberShrinked: Int): RFFeatureSizer = {
    if (binNumberShrinked != numberBin) {
      new RFFeatureSizerSimpleModelSelection(numberBin, numClasses, categoricalFeatureInfo, binNumberShrinked)
    } else {
      generateRFSizer(numClasses)
    }
  }
}
