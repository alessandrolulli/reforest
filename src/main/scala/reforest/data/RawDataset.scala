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

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import reforest.TypeInfo
import reforest.rf.RFCategoryInfo
import reforest.util.{CCUtil, GCInstrumented}
import reforest.data.load.DataLoad
import reforest.rf.parameter.RFParameter

import scala.reflect.ClassTag

/**
  * This class stores the raw data.
  *
  * @param rawData      the whole raw dataset
  * @param trainingData a subset of the raw dataset used for the training phase
  * @param testingData  a subset of the raw dataset used for the testing phase
  * @param checkingData a subset of the raw dataset used for the checking phase during model selection
  * @tparam T raw data type
  * @tparam U working data type
  */
class RawDataset[T, U](val rawData: RDD[RawDataLabeled[T, U]],
                       val trainingData: RDD[RawDataLabeled[T, U]],
                       val testingData: RDD[RawDataLabeled[T, U]],
                       val checkingData: RDD[RawDataLabeled[T, U]]) extends Serializable {

  def this(rawData: RDD[RawDataLabeled[T, U]],
           trainingData: RDD[RawDataLabeled[T, U]],
           testingData: RDD[RawDataLabeled[T, U]]) = this(rawData, trainingData, testingData, testingData)

}

/**
  * The implementation to be used for the model selection
  *
  * @param rawData      the whole raw dataset
  * @param trainingData a subset of the raw dataset used for the training phase
  * @param testingData  a subset of the raw dataset used for the testing phase
  * @param checkingData a subset of the raw dataset used for the checking phase during model selection
  * @tparam T raw data type
  * @tparam U working data type
  */
class RawDatasetModelSelection[T, U](rawData: RDD[RawDataLabeled[T, U]],
                                     trainingData: RDD[RawDataLabeled[T, U]],
                                     testingData: RDD[RawDataLabeled[T, U]],
                                     checkingData: RDD[RawDataLabeled[T, U]]) extends RawDataset[T, U](rawData, trainingData, testingData, checkingData) {

}

/**
  * The builder for the RawDataset class
  *
  * @tparam T raw data type
  * @tparam U working data type
  */
class RawDatasetBuilder[T: ClassTag, U: ClassTag] {
  var numFeatures = 0
  var minPartition = 0
  var splitSizeTrainingData = 0.6
  var splitSizeCheckingData = 0.2
  var splitSizeTestingData = 0.2
  var storageLevel = StorageLevel.MEMORY_ONLY
  var property: Option[Broadcast[RFParameter]] = Option.empty
  var typeInfoBC: Option[Broadcast[TypeInfo[T]]] = Option.empty
  var instrumented: Option[Broadcast[GCInstrumented]] = Option.empty
  var categoricalInfoBC: Option[Broadcast[RFCategoryInfo]] = Option.empty

  /**
    * Retrieve the correct data loader
    *
    * @return the data loader specialized for the dataset
    */
  def getDataLoad: DataLoad[T, U] = {
    CCUtil.getDataLoader[T, U](property.get.value,
      typeInfoBC.get,
      instrumented.get,
      categoricalInfoBC.get)
  }

  /**
    * Load the data using the specialized data loader
    *
    * @param sc           the Spark context
    * @param path         the path to the dataset
    * @param numFeature   the number of features in the dataset
    * @param minPartition the minimum number of partitions for the RDD
    * @return the loaded raw dataset
    */
  def load(sc: SparkContext, path: String, numFeature: Int, minPartition: Int): RDD[RawDataLabeled[T, U]] = getDataLoad.loadFile(sc, path, numFeature, minPartition)

  /**
    * Build the RawDataset class
    *
    * @param sc   the Spark context
    * @param path the path to the dataset
    * @return the RawDataset class
    */
  def build(sc: SparkContext, path: String): RawDataset[T, U] = {

    val loaded: RDD[RawDataLabeled[T, U]] = load(sc, path, numFeatures, minPartition)
    val result = loaded.randomSplit(Array(splitSizeTrainingData, splitSizeCheckingData, splitSizeTestingData), 0)

    new RawDataset[T, U](loaded, result(0), result(2))
  }
}

/**
  * The builder for the RawDataset class when using random rotations
  *
  * @tparam T raw data type
  * @tparam U working data type
  */
class RawDatasetBuilderRotation[T: ClassTag, U: ClassTag] extends RawDatasetBuilder[T, U] {

  override def build(sc: SparkContext, path: String): RawDataset[T, U] = {
    val rawData = load(sc, path, numFeatures, minPartition)

    val scaling = new ScalingBasic[T, U](sc, typeInfoBC.get, numFeatures, rawData)

    val a = rawData.map(t => scaling.scale(t)).randomSplit(Array(splitSizeTrainingData, (1 - splitSizeTrainingData)), 0)

    val trainingData = a(0)
    val testingData = a(1)

    new RawDataset[T, U](rawData, trainingData, testingData)
  }
}

/**
  * The builder for the RawDataset class when using model selection
  *
  * @param innerRawDataset the already built RawDataset class
  * @tparam T raw data type
  * @tparam U working data type
  */
class RawDatasetBuilderModelSelection[T: ClassTag, U: ClassTag](val innerRawDataset: RawDataset[T, U]) extends RawDatasetBuilder[T, U] {
  splitSizeTrainingData = 0.6
  splitSizeCheckingData = 0.2
  splitSizeTestingData = 0.2

  override def build(sc: SparkContext, path: String): RawDataset[T, U] = {
    val result = innerRawDataset.rawData.randomSplit(Array(splitSizeTrainingData, splitSizeCheckingData, splitSizeTestingData), 0)

    new RawDatasetModelSelection[T, U](innerRawDataset.rawData, result(0), result(2), result(1))
  }
}
