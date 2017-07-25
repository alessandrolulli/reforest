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

package reforest.data.tree

import reforest.rf.feature.RFStrategyFeatureCUSTOM
import reforest.rf.parameter.RFParameter
import reforest.rf.split.RFSplitterManager

import scala.collection.mutable.ListBuffer

/**
  *This class handles all the forests that must be generated. This is particularly useful when doing the model
  * selection. It stores a forest for each combination of the parameters.
  * @param parameter the configuration parameter
  * @param splitterManager the utility to define the splits of each feature
  * @param forestArrayPreComputed an array with the forest already initialized
  * @tparam T raw data type
  * @tparam U working data type
  */
class ForestManager[T, U](parameter: RFParameter,
                          val splitterManager: RFSplitterManager[T, U],
                          forestArrayPreComputed: Option[Array[Forest[T, U]]] = Option.empty) extends Serializable {

  def this(parameter: RFParameter,
           splitterManager: RFSplitterManager[T, U],
           forestArrayPreComputed: Array[Forest[T, U]]) = this(parameter, splitterManager, Some(forestArrayPreComputed))

  private val forestArray: Array[Forest[T, U]] = if (forestArrayPreComputed.isDefined) forestArrayPreComputed.get else generateForest(parameter)

  /**
    * Return all the forests stored in the forest manager
    * @return all the forest stored
    */
  def getForest: Array[Forest[T, U]] = forestArray

  /**
    * To retrieve the parameters used to initialize the forest manager
    * @return the configuration parameters
    */
  def getParameter: RFParameter = parameter

  /**
    * It returns the maximum number of trees in a forest
    * @return the number of trees
    */
  def getNumTrees : Int = forestArray.map(t => t.numTrees).max

  /**
    * Merge the forest manager with another forest manager in place
    * @param other another forest manager to use for the merging
    */
  def merge(other : ForestManager[T, U]) = {
    var forestId = 0
    while(forestId < forestArray.length) {
      forestArray(forestId).merge(other.forestArray(forestId))
      forestId += 1
    }
  }

  private def generateForest(parameter: RFParameter) = {
    val toReturn: ListBuffer[Forest[T, U]] = ListBuffer()

    for (numFeatureSet <- parameter.featureMultiplierPerNode.indices) {
      for (numBinSet <- parameter.binNumber.indices) {

        toReturn += new ForestFull[T, U](parameter.getMaxNumTrees,
          parameter.getMaxDepth,
          parameter.binNumber(numBinSet),
          new RFStrategyFeatureCUSTOM(parameter.numFeatures, Math.ceil(parameter.strategyFeature.getFeaturePerNodeNumber * parameter.featureMultiplierPerNode(numFeatureSet)).toInt),
          splitterManager.generateRFSizer(parameter.numClasses, parameter.binNumber(numBinSet)))
      }
    }

    toReturn.toArray
  }
}
