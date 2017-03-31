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
import reforest.dataTree.TreeNode
import reforest.rf.rotation.RFRotationMatrix

import scala.collection.mutable.ListBuffer

trait RFModel[T, U] extends Serializable {
  def predict(point : RawDataLabeled[T, U]) : Int = {
    predict(point.features)
  }

  def predict(point : RawData[T, U]) : Int = {
    predictDetails(point).maxBy(_._2)._1
  }

  def predictDetails(point : RawData[T, U]) : Array[(Int, Int)] // (class, #vote)
}

class RFModelStandard[T, U](val forest : Broadcast[Array[TreeNode[T, U]]], val typeInfo : Broadcast[TypeInfo[T]]) extends RFModel[T, U] {

  override def predictDetails(point : RawData[T, U]) : Array[(Int, Int)] = {
    forest.value.map(t => t.predict(point, typeInfo.value)).groupBy(identity).mapValues(_.size).toArray
  }
}

class RFModelRotate[T, U](val forest : Broadcast[Array[TreeNode[T, U]]],
                          val typeInfo : Broadcast[TypeInfo[T]],
                          val rotationMatrix : Broadcast[RFRotationMatrix[T, U]]) extends RFModel[T, U] {

  override def predictDetails(point : RawData[T, U]) : Array[(Int, Int)] = {
    val rotatedData = rotationMatrix.value.rotateRawData(point)
    forest.value.map(t => t.predict(rotatedData, typeInfo.value)).groupBy(identity).mapValues(_.size).toArray
  }
}

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
