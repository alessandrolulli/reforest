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

package reforest.data

/**
  * It represents the data that are stored statically in the main memory to compute the random forest.
  *
  * @tparam U working data type
  */
trait StaticData[U] extends Serializable {
  /**
    * It returns the class label of the data
    *
    * @return the class label of the data
    */
  def getLabel: Byte

  /**
    * It returns the working data. The discretized representation of the raw data
    *
    * @param treeId The tree identifier
    * @return It returns the working data for the tree treeId
    */
  def getWorkingData(treeId: Int): WorkingData[U]

  /**
    * It returns the bagginf for this data in a given tree
    *
    * @param treeId The tree identifier
    * @return It returns the bagging for the tree treeId
    */
  def getBagging(treeId: Int): Int

  /**
    * Substitute the bagging with the bagging passed as argument
    *
    * @param bagging the newly bagging
    * @return a new StaticData with the passed bagging
    */
  def applyBagging(bagging: Array[Byte]): StaticData[U]
}

/**
  * It represent the basic static data where the working data is the same for all the trees and the bagging is represented as an array
  *
  * @param label       The label for the class
  * @param workingData The working data
  * @param bagging     The bagging
  * @tparam U working data type
  */
class StaticDataClassic[U](label: Byte, workingData: WorkingData[U], bagging: Array[Byte]) extends StaticData[U] {
  def getLabel: Byte = label

  def getWorkingData(treeId: Int): WorkingData[U] = workingData

  def getBagging(treeId: Int) = bagging(treeId)

  def applyBagging(baggingArgs: Array[Byte]): StaticData[U] = new StaticDataClassic[U](label, workingData, baggingArgs)
}

/**
  * Static data when computing multiple rotations at the same time
  *
  * @param label       The label for the class
  * @param workingData Each rotation has a different working data
  * @tparam U working data type
  */
class StaticDataRotation[U](label: Byte, workingData: Array[WorkingData[U]]) extends StaticData[U] {
  def getLabel: Byte = label

  def getWorkingData(treeId: Int): WorkingData[U] = workingData(treeId)

  def getBagging(treeId: Int) = 1

  def applyBagging(baggingArgs: Array[Byte]): StaticData[U] = this
}

/**
  * Static data when computing one rotation at a time
  *
  * @param label       The label for the class
  * @param workingData The working data for the rotation under computation
  * @tparam U working data type
  */
class StaticDataRotationSingle[U](label: Byte, workingData: WorkingData[U]) extends StaticData[U] {
  def getLabel: Byte = label

  def getWorkingData(treeId: Int): WorkingData[U] = workingData

  def getBagging(treeId: Int) = 1

  def applyBagging(baggingArgs: Array[Byte]): StaticData[U] = this
}
