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

import reforest.rf.{RFCategoryInfo, RFFeatureSizer, RFFeatureSizerSimple}

trait RFSplitterManager[T, U] extends Serializable {
  def getBinNumber(idFeature: Int, idTree : Int = 0): U
  def getBin(idFeature: Int, value: T, idTree : Int = 0): U
  def getRealCut(idFeature: Int, cut: U, idTree : Int = 0): T
  def getSplitter(macroIteration : Int, idTree : Int = 0) : RFSplitter[T, U]
  def generateRFSizer(numClasses: Int): RFFeatureSizer
}

class RFSplitterManagerSingle[T, U](splitter : RFSplitter[T, U]) extends RFSplitterManager[T, U] {
  def getBinNumber(idFeature: Int, idTree : Int = 0): U = splitter.getBinNumber(idFeature)
  def getBin(idFeature: Int, value: T, idTree : Int = 0): U = splitter.getBin(idFeature, value)
  def getRealCut(idFeature: Int, cut: U, idTree : Int = 0): T = splitter.getRealCut(idFeature, cut)
  def getSplitter(macroIteration : Int, idTree : Int = 0) : RFSplitter[T, U] = splitter
  def generateRFSizer(numClasses: Int): RFFeatureSizer = splitter.generateRFSizer(numClasses)
}

class RFSplitterManagerCollection[T, U](splitter : Array[RFSplitter[T, U]],
                                        binNumber : Int,
                                        numTrees : Int,
                                        numMacroIteration : Int,
                                        categoryInfo : RFCategoryInfo) extends RFSplitterManager[T, U] {
  def getBinNumber(idFeature: Int, idTree : Int = 0): U = splitter(idTree/numMacroIteration).getBinNumber(idFeature)
  def getBin(idFeature: Int, value: T, idTree : Int = 0): U = splitter(idTree/numMacroIteration).getBin(idFeature, value)
  def getRealCut(idFeature: Int, cut: U, idTree : Int = 0): T = splitter(idTree/numMacroIteration).getRealCut(idFeature, cut)
  def getSplitter(macroIteration : Int, idTree : Int = 0) : RFSplitter[T, U] = splitter(macroIteration)
  def generateRFSizer(numClasses: Int): RFFeatureSizer = new RFFeatureSizerSimple(binNumber, numClasses, categoryInfo)
}
