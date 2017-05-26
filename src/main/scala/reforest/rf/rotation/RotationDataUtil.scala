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

package reforest.rf.rotation

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import reforest.TypeInfo
import reforest.data.load.{ARFFUtil, DataLoad}
import reforest.data.{RawDataLabeled, ScalingBasic}
import reforest.rf.RFProperty

import scala.reflect.ClassTag

/**
  * It provides utility for the random rotation matrix
  *
  * @param sc            the Spark Context
  * @param property      the ReForeSt property
  * @param typeInfo      the type info for the raw data
  * @param minPartitions the minimum number of partitions for the RDD
  * @tparam T raw data type
  * @tparam U working data type
  */
class RotationDataUtil[T: ClassTag, U: ClassTag](@transient private val sc: SparkContext,
                                                 property: RFProperty,
                                                 typeInfo: Broadcast[TypeInfo[T]],
                                                 minPartitions: Int) extends Serializable {

  private var scaledDataTraining: Option[RDD[RawDataLabeled[T, U]]] = Option.empty
  private var scaledDataTesting: Option[RDD[RawDataLabeled[T, U]]] = Option.empty
  private var count = 0

  val matrices = generateMatrices(property.numRotation)

  private def generateMatrices(amount: Int) = {
    sc.parallelize(Array.tabulate(amount)(i => i)).map(i => new RFRotationMatrix[T, U](property.featureNumber, typeInfo, i)).collect()
  }

  /**
    * Rotate the dataset
    *
    * @param training the dataset to rotate
    * @return the rotated dataset
    */
  def rotate(training: RDD[RawDataLabeled[T, U]]) = {
    val rotationMatrix = matrices(count)
    count += 1

    (training.map(t => rotationMatrix.rotate(t)), rotationMatrix)
  }

  /**
    * Get the dataset scaled
    *
    * @param splitSize the ratio to split training and testing dataset
    * @param dataLoad  the utility to load the dataset from file
    * @return the scaled training and testing dataset
    */
  def getScaledData(splitSize: Double, dataLoad: DataLoad[T, U]): (RDD[RawDataLabeled[T, U]], RDD[RawDataLabeled[T, U]]) = {
    if (scaledDataTesting.isDefined) {
      (scaledDataTraining.get, scaledDataTesting.get)
    } else {
      val rawData = dataLoad.loadFile(sc, property.dataset, property.featureNumber, minPartitions)

      val scaling = new ScalingBasic[T, U](sc, typeInfo, property.featureNumber, rawData)

      val a = rawData.map(t => scaling.scale(t)).randomSplit(Array(splitSize, (1 - splitSize)), 0)

      scaledDataTraining = Some(a(0))
      scaledDataTesting = Some(a(1))

      scaledDataTraining.get.persist(property.storageLevel).count()
      scaledDataTesting.get.persist(property.storageLevel).count()

      (scaledDataTraining.get, scaledDataTesting.get)
    }
  }
}
