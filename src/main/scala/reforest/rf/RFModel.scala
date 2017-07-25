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

import org.apache.spark.broadcast.Broadcast
import reforest.TypeInfo
import reforest.data.{RawData, RawDataLabeled}
import reforest.data.tree.Forest
import reforest.rf.rotation.RFRotationMatrix

import scala.collection.mutable.ListBuffer

/**
  * It predicts the class for the raw data in the testing set
  *
  * @tparam T raw data type
  * @tparam U working data type
  */
trait RFModel[T, U] extends Serializable {

  /**
    * The bin number used to learn this model
    *
    * @return the number of bin
    */
  def getBinNumber: Int

  /**
    * The maximum depth used to learn this model
    *
    * @return the maximum depth
    */
  def getDepth: Int

  /**
    * The number of tress used to learn this model
    *
    * @return the number of trees
    */
  def getNumTrees: Int

  /**
    * The number of features used in each node to detect the split to learn this model
    *
    * @return the number of features used in each node
    */
  def getFeaturePerNode: Int

  /**
    * It predicts the label for the given data
    *
    * @param point    the data to predict
    * @param maxDepth the maximum depth to visit
    * @return the predicted label
    */
  def predict(point: RawData[T, U], maxDepth: Int = Int.MaxValue, maxTrees: Int = Int.MaxValue): Int = {
    predictDetails(maxDepth, maxTrees, point).maxBy(_._2)._1
  }

  /**
    * It collects the amount of trees voting for each class
    *
    * @param maxDepth the maximum depth to visit
    * @param point    the data to predict
    * @return an array with the amount of votes for each class
    */
  def predictDetails(maxDepth: Int, maxTrees: Int, point: RawData[T, U]): Array[(Int, Int)] // (class, #vote)

  override def toString: String = "(BINNUMBER "+getBinNumber+")(FEATURE "+getFeaturePerNode+")(DEPTH "+getDepth+")(TREES "+getNumTrees+")"
}

/**
  * The base implementation for a RFModel
  *
  * @param forest     the forest that must be used to predict the labels
  * @param typeInfo   the type information for the raw data
  * @param numClasses the number of classes in the dataset
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFModelStandard[T, U](val forest: Broadcast[Forest[T, U]],
                            val typeInfo: Broadcast[TypeInfo[T]],
                            numClasses: Int) extends RFModel[T, U] {
  override def getBinNumber: Int = forest.value.binNumber

  override def getDepth: Int = forest.value.maxDepth

  override def getNumTrees: Int = forest.value.numTrees

  override def getFeaturePerNode: Int = forest.value.featurePerNode.getFeaturePerNodeNumber

  override def predictDetails(maxDepth: Int, maxTrees: Int, point: RawData[T, U]): Array[(Int, Int)] = {
    val toReturn: Array[Int] = new Array(numClasses)
    var count = 0
    while (count < forest.value.numTrees && count < maxTrees) {
      val predicted = forest.value.predict(maxDepth, count, point, typeInfo.value)
      if (predicted >= 0) {
        toReturn(predicted) += 1
      }
      count += 1
    }
    toReturn.zipWithIndex.map(_.swap)
  }
}

/**
  * The model when using the rotation of ReForeSt.
  *
  * @param forest         the forest
  * @param typeInfo       the type information of the raw data
  * @param rotationMatrix the rotation matrix for the given forest
  * @param numClasses     number of classes in the dataset
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFModelRotate[T, U](val forest: Broadcast[Forest[T, U]],
                          val typeInfo: Broadcast[TypeInfo[T]],
                          val rotationMatrix: Broadcast[RFRotationMatrix[T, U]],
                          numClasses: Int) extends RFModel[T, U] {

  override def getBinNumber: Int = forest.value.binNumber

  override def getDepth: Int = forest.value.maxDepth

  override def getNumTrees: Int = forest.value.numTrees

  override def getFeaturePerNode: Int = forest.value.featurePerNode.getFeaturePerNodeNumber

  override def predictDetails(maxDepth: Int, maxTrees: Int, point: RawData[T, U]): Array[(Int, Int)] = {
    val rotatedData = rotationMatrix.value.rotateRawData(point)
    val toReturn: Array[Int] = new Array(numClasses)
    var count = 0
    while (count < forest.value.numTrees && count < maxTrees) {
      val index = forest.value.predict(maxDepth, count, rotatedData, typeInfo.value)

      if (index >= 0) {
        toReturn(index) += 1
      }

      count += 1
    }
    toReturn.zipWithIndex.map(_.swap)
  }
}

/**
  * It aggregates multiple RFModel.
  * It is possible to add models incrementally.
  *
  * @param numClasses number of classes in the dataset
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFModelAggregator[T, U](val numClasses: Int) extends RFModel[T, U] {

  var modelList: ListBuffer[RFModel[T, U]] = new ListBuffer[RFModel[T, U]]()

  override def getBinNumber: Int = modelList.map(m => m.getBinNumber).max

  override def getDepth: Int = modelList.map(m => m.getDepth).max

  override def getNumTrees: Int = modelList.map(m => m.getNumTrees).sum

  override def getFeaturePerNode: Int = modelList.map(m => m.getFeaturePerNode).max

  def addModel(model: RFModel[T, U]): Unit = {
    modelList += model
  }

  override def predictDetails(maxDepth: Int, maxTrees: Int, point: RawData[T, U]): Array[(Int, Int)] = {
    val classAccumulator = new Array[Int](numClasses)

    var treesToUse = maxTrees
    for (model <- modelList) {
      val array = model.predictDetails(maxDepth, treesToUse, point)
      for ((index, vote) <- array) {
        classAccumulator(index) += vote
      }
      treesToUse = treesToUse - model.getNumTrees
    }

    classAccumulator.zipWithIndex.map(_.swap)
  }
}

/**
  * This model overrides the parameters used in the contained model to predict the labels
  *
  * @param model the underlying model
  * @param depth the maximum depth to use to predict the labels
  * @param trees the maximum number of trees to use to predict the labels
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFModelFixedParameter[T, U](val model: RFModel[T, U], val depth: Int, val trees: Int) extends RFModel[T, U] {
  override def getBinNumber: Int = model.getBinNumber

  override def getDepth: Int = depth

  override def getNumTrees: Int = trees

  override def getFeaturePerNode: Int = model.getFeaturePerNode

  override def predictDetails(maxDepth: Int, maxTrees: Int, point: RawData[T, U]): Array[(Int, Int)] = {
    model.predictDetails(depth, trees, point)
  }
}