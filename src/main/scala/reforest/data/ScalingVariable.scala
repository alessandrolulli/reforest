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
import reforest.TypeInfo

import scala.reflect.ClassTag

/**
  * It scales the value of the raw data according to different methodologies
  * @tparam T raw data type
  * @tparam U working data type
  */
trait ScalingVariable[T, U] extends Serializable {

  /**
    * It scales the data passed as argument
    * @param data The value to be scaled
    * @return The scaled data
    */
  def scale(data: RawDataLabeled[T, U]): RawDataLabeled[T, U]

}

/**
  * It scales the values according to the Basic Scaling of Blaser et al. "Random rotation ensembles".
  * Numeric values are scaled to [0, 1] using the min and max values.
  * @param sc The Spark Context
  * @param typeInfo The type information about the raw data
  * @param featureNumber The number of feature in the dataset
  * @param input The raw dataset
  * @tparam T raw data type
  * @tparam U working data type
  */
class ScalingBasic[T : ClassTag, U : ClassTag](@transient private val sc: SparkContext,
                         typeInfo: Broadcast[TypeInfo[T]],
                         featureNumber: Int,
                         input: RDD[RawDataLabeled[T, U]]) extends ScalingVariable[T, U] {

  private val scaling: Broadcast[scala.collection.Map[Int, (T, T)]] = sc.broadcast(init())

  private def scaleValue(index: Int, value: T): T = {
    val (min, max) = scaling.value(index)
    val doubleValue = typeInfo.value.toDouble(value)
    typeInfo.value.fromDouble(Math.min(1, Math.max(0, (doubleValue - typeInfo.value.toDouble(min)) / (typeInfo.value.toDouble(max) - typeInfo.value.toDouble(min)))))
  }

  override def scale(data: RawDataLabeled[T, U]): RawDataLabeled[T, U] = {
    val densed = data.features.toDense
    val values = new Array[T](densed.size)
    var count = 0

    while (count < values.length) {
      values(count) = scaleValue(count, densed(count))
      count += 1
    }

    new RawDataLabeled(data.label, new RawDataDense(values, densed.nan))
  }

  private def init(): scala.collection.Map[Int, (T, T)] = {

    input.mapPartitions(it => {
      val min = Array.fill(featureNumber)(typeInfo.value.maxValue)
      val max = Array.fill(featureNumber)(typeInfo.value.minValue)

      def setMinMax(index: Int, value: T): Unit = {
        if (typeInfo.value.isMinOrEqual(value, min(index))) {
          min(index) = value
        }
        if (typeInfo.value.isMinOrEqual(max(index), value)) {
          max(index) = value
        }
      }

      it.foreach(t => {
        t.features.foreachActive(setMinMax)
      })

      min.zip(max).zipWithIndex.map(_.swap).toIterator
    }).reduceByKey((a, b) => (typeInfo.value.min(a._1, b._1), typeInfo.value.max(a._2, b._2))).collectAsMap()
  }
}
