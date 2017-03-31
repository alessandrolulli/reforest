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
import reforest.rf.{RFCategoryInfo, RFFeatureSizer, RFFeatureSizerSimple, RFFeatureSizerSpecialized}

import scala.collection.Map

trait RFSplitter[T, U] extends Serializable {

  def getBinNumber(idFeature: Int): U

  def getBin(index: Int, value: T): U

  def getRealCut(index: Int, cut: U): T

  def generateRFSizer(numClasses: Int): RFFeatureSizer
}

class RFSplitterSpecialized[T, U](split: Map[Int, Array[T]],
                                  typeInfo: TypeInfo[T],
                                  typeInfoWorking: TypeInfo[U],
                                  categoricalFeatureInfo: RFCategoryInfo) extends RFSplitter[T, U] {

  override def getBinNumber(idFeature: Int): U = {
    if (categoricalFeatureInfo.isCategorical(idFeature))
      typeInfoWorking.fromInt(categoricalFeatureInfo.getArity(idFeature))
    else
      typeInfoWorking.fromInt(split(idFeature).length)
  }

  override def getBin(index: Int, value: T): U = {
    if (typeInfo.isNOTvalidDefined && !typeInfo.isValidForBIN(value)) {
      typeInfoWorking.zero
    } else {
      if (categoricalFeatureInfo.isCategorical(index)) {
        typeInfoWorking.fromInt(typeInfo.toInt(value) + 1)
      } else {
        val split2 = split.get(index).get

        val idx = typeInfo.getIndex(split2, value)
        val idx2 = -idx - 1
        typeInfoWorking.fromInt((Math.min(Math.max(idx, idx2), split2.length - 1) + 1))
      }
    }
  }

  override def getRealCut(index: Int, cut: U): T = {
    typeInfo.getRealCut(typeInfoWorking.toInt(cut), split(index))
  }

  override def generateRFSizer(numClasses: Int): RFFeatureSizer = {
    new RFFeatureSizerSpecialized(split.map(t => (t._1, t._2.size)), numClasses, categoricalFeatureInfo)
  }
}

class RFSplitterSimple[T, U](min: T,
                             max: T,
                             typeInfo: TypeInfo[T],
                             typeInfoWorking: TypeInfo[U],
                             numberBin: Int,
                             categoricalFeatureInfo: RFCategoryInfo) extends RFSplitter[T, U] {

  val simpleSplitter = typeInfo.getSimpleSplit(min, max, numberBin, typeInfoWorking)
  val simpleSplitterInverted = typeInfo.getSimpleSplitInverted(min, max, numberBin, typeInfoWorking)

  override def getBinNumber(idFeature: Int): U = {
    if (categoricalFeatureInfo.isCategorical(idFeature))
      typeInfoWorking.fromInt(categoricalFeatureInfo.getArity(idFeature))
    else
      typeInfoWorking.fromInt(numberBin)
  }

  override def getBin(index: Int, value: T): U = {
    if (categoricalFeatureInfo.isCategorical(index)) {
      typeInfoWorking.fromInt(typeInfo.toInt(value) + 1)
    } else {
      simpleSplitter(value)
    }
  }

  override def getRealCut(index: Int, cut: U): T = {
    simpleSplitterInverted(cut)
  }

  override def generateRFSizer(numClasses: Int): RFFeatureSizer = {
    new RFFeatureSizerSimple(numberBin, numClasses, categoricalFeatureInfo)
  }
}
