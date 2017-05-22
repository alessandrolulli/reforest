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
import reforest.dataTree.Forest
import reforest.rf.rotation.RFRotationMatrix

import scala.collection.mutable.ListBuffer

/**
  * It predicts the class for the raw data in the testing set
  * @tparam T raw data type
  * @tparam U working data type
  */
trait RFModel[T, U] extends Serializable {
  /**
    * It predicts the label for the given data
    * @param point the data to predict
    * @return the predicted label
    */
  def predict(point : RawDataLabeled[T, U]) : Int = {
    predict(point.features)
  }

  /**
    * It predicts the label for the given data
    * @param point the data to predict
    * @return the predicted label
    */
  def predict(point : RawData[T, U]) : Int = {
    predictDetails(point).maxBy(_._2)._1
  }

  /**
    * It collects the amount of trees voting for each class
    * @param point the data to predict
    * @return an array with the amount of votes for each class
    */
  def predictDetails(point : RawData[T, U]) : Array[(Int, Int)] // (class, #vote)
}

class RFModelStandard[T, U](val forest : Broadcast[Forest[T, U]],
                            val typeInfo : Broadcast[TypeInfo[T]],
                            numClasses : Int) extends RFModel[T, U] {

  override def predictDetails(point : RawData[T, U]) : Array[(Int, Int)] = {
    val toReturn : Array[Int] = new Array(numClasses)
    var count = 0
    while(count < forest.value.numTrees) {
      toReturn(forest.value.predict(count, point, typeInfo.value)) += 1
      count += 1
    }
    toReturn.zipWithIndex.map(_.swap)
  }
}

/**
  * The model when using the rotation of ReForeSt.
  * @param forest the forest
  * @param typeInfo
  * @param rotationMatrix the rotation matrix for the given forest
  * @param numClasses number of classes in the dataset
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFModelRotate[T, U](val forest : Broadcast[Forest[T, U]],
                          val typeInfo : Broadcast[TypeInfo[T]],
                          val rotationMatrix : Broadcast[RFRotationMatrix[T, U]],
                          numClasses : Int) extends RFModel[T, U] {

  override def predictDetails(point : RawData[T, U]) : Array[(Int, Int)] = {
    val rotatedData = rotationMatrix.value.rotateRawData(point)
    val toReturn : Array[Int] = new Array(numClasses)
    var count = 0
    while(count < forest.value.numTrees) {
      val index = forest.value.predict(count, rotatedData, typeInfo.value)
      toReturn(index) += 1
      count += 1
    }
    toReturn.zipWithIndex.map(_.swap)
  }
}

/**
  * It aggregates multiple RFModel.
  * It is possible to add models incrementally.
  * @param numClasses number of classes in the dataset
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFModelAggregator[T, U](val numClasses : Int) extends RFModel[T, U] {

  var modelList : ListBuffer[RFModel[T, U]] = new ListBuffer[RFModel[T, U]]()

  def addModel(model : RFModel[T, U]) = {
    modelList += model
  }

  override def predictDetails(point : RawData[T, U]) : Array[(Int, Int)] = {
    val classAccumulator = new Array[Int](numClasses)

    for(model <- modelList) {
      val array = model.predictDetails(point)
      for((index, vote) <- array) {
        if(index>=0)
        classAccumulator(index) += vote
      }
    }

    classAccumulator.zipWithIndex.map(_.swap)
  }
}
