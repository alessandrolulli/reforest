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

import reforest.rf.RFCategoryInfo
import reforest.rf.feature.{RFFeatureSizer, RFFeatureSizerSimple, RFFeatureSizerSimpleModelSelection}

/**
  * The manager to collect the information about how the data are discretized
  *
  * @tparam T raw data type
  * @tparam U working data type
  */
trait RFSplitterManager[T, U] extends Serializable {

  /**
    * The number of bin used for the feature passed as argument. It is always <= number of configured bin
    *
    * @param idFeature the feature identifier
    * @param idTree    the index of the tree
    * @return the number of bin for the feature
    */
  def getBinNumber(idFeature: Int, idTree: Int = 0): U

  /**
    * Discretize a value to the correct bin number
    *
    * @param idFeature the feature identifier
    * @param value     the raw value
    * @param idTree    the index of the tree
    * @return the discretized value
    */
  def getBin(idFeature: Int, value: T, idTree: Int = 0): U

  /**
    * Retrieve the raw value from a bin number
    *
    * @param idFeature the feature identifier
    * @param cut       the bin number of the cut
    * @param idTree    the index of the tree
    * @return the raw value
    */
  def getRealCut(idFeature: Int, cut: U, idTree: Int = 0): T

  /**
    * Get the splitter for the given macro iteration
    *
    * @param macroIteration the macro iteration number
    * @param idTree         the identifier of the tree
    * @return the specialized splitter for the given macro iteration and tree
    */
  def getSplitter(macroIteration: Int, idTree: Int = 0): RFSplitter[T, U]

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
  * The implementation of RFSplitterManager where each tree has the same RFSplitter
  *
  * @param splitter
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFSplitterManagerSingle[T, U](splitter: RFSplitter[T, U]) extends RFSplitterManager[T, U] {
  override def getBinNumber(idFeature: Int, idTree: Int = 0): U = splitter.getBinNumber(idFeature)

  override def getBin(idFeature: Int, value: T, idTree: Int = 0): U = splitter.getBin(idFeature, value)

  override def getRealCut(idFeature: Int, cut: U, idTree: Int = 0): T = splitter.getRealCut(idFeature, cut)

  override def getSplitter(macroIteration: Int, idTree: Int = 0): RFSplitter[T, U] = splitter

  override def generateRFSizer(numClasses: Int): RFFeatureSizer = splitter.generateRFSizer(numClasses)

  override def generateRFSizer(numClasses: Int, binNumberShrinked: Int): RFFeatureSizer = splitter.generateRFSizer(numClasses, binNumberShrinked)
}

/**
  * The implementation of RFSplitterManager where each macro iteration has a different RFSplitter
  *
  * @param splitter a list of RFSplitter (one for each macro iteration)
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFSplitterManagerCollection[T, U](splitter: Array[RFSplitter[T, U]],
                                        binNumber: Int,
                                        numTrees: Int,
                                        numMacroIteration: Int,
                                        categoryInfo: RFCategoryInfo) extends RFSplitterManager[T, U] {
  override def getBinNumber(idFeature: Int, idTree: Int = 0): U = splitter(idTree / numMacroIteration).getBinNumber(idFeature)

  override def getBin(idFeature: Int, value: T, idTree: Int = 0): U = splitter(idTree / numMacroIteration).getBin(idFeature, value)

  override def getRealCut(idFeature: Int, cut: U, idTree: Int = 0): T = splitter(idTree / numMacroIteration).getRealCut(idFeature, cut)

  override def getSplitter(macroIteration: Int, idTree: Int = 0): RFSplitter[T, U] = splitter(macroIteration)

  override def generateRFSizer(numClasses: Int): RFFeatureSizer = new RFFeatureSizerSimple(binNumber, numClasses, categoryInfo)

  override def generateRFSizer(numClasses: Int, binNumberShrinked: Int): RFFeatureSizer = new RFFeatureSizerSimpleModelSelection(binNumber, numClasses, categoryInfo, binNumberShrinked)
}
