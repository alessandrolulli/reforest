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

package reforest.data.load

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import reforest.TypeInfo
import reforest.data.{RawData, RawDataLabeled}
import reforest.rf.RFCategoryInfo
import reforest.util.GCInstrumented

import scala.reflect.ClassTag

/**
  * Forked from Apache Spark MLlib
  * Load data in LibSVM format
  *
  * @param typeInfo     the type information of the raw data
  * @param instrumented the instrumentation of the GC
  * @param categoryInfo the information for the categorical features
  * @tparam T raw data type
  * @tparam U working data type
  */
class LibSVMUtil[T: ClassTag, U: ClassTag](typeInfo: Broadcast[TypeInfo[T]],
                                           instrumented: Broadcast[GCInstrumented],
                                           categoryInfo: Broadcast[RFCategoryInfo]) extends DataLoad[T, U] {

  override def loadFile(sc: SparkContext,
                        path: String,
                        numFeatures: Int,
                        minPartitions: Int): RDD[RawDataLabeled[T, U]] = {
    val parsed = parseLibSVMFile(sc, path, minPartitions)
    instrumented.value.gcALL

    parsed.map {
      case (label, indices, values) =>
        RawDataLabeled(label, RawData.sparse[T, U](numFeatures, indices, values, typeInfo.value.NaN).compressed)
    }
  }

  private def parseLibSVMFile(sc: SparkContext,
                              path: String,
                              minPartitions: Int): RDD[(Double, Array[Int], Array[T])] = {
    sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .mapPartitions(it => {
        val toReturn = it.map(u => parseLibSVMRecord(u))
        instrumented.value.gc()
        toReturn
      })
  }

  private def parseLibSVMRecord(line: String): (Double, Array[Int], Array[T]) = {
    val items = line.split(' ')
    val label = Math.max(items.head.toDouble, 0)
    val (indices, values) = items.tail.filter(_.nonEmpty).map {
      item =>
        val indexAndValue = item.split(':')
        val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
      val value = typeInfo.value.fromString(indexAndValue(1))

        if (categoryInfo.value.isCategorical(index)) {
          (index, typeInfo.value.fromInt(categoryInfo.value.rawRemapping(typeInfo.value.toInt(value))))
        } else {
          (index, value)
        }
    }.unzip

    // check if indices are one-based and in ascending order
    var previous = -1
    var i = 0
    val indicesLength = indices.length
    while (i < indicesLength) {
      val current = indices(i)
      require(current > previous, s"indices should be one-based and in ascending order;"
        + " found current=$current, previous=$previous; line=\"$line\"")
      previous = current
      i += 1
    }
    (label, indices, values)
  }
}
