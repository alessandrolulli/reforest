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

class ARFFUtil[T: ClassTag, U: ClassTag](typeInfo: Broadcast[TypeInfo[T]],
                                         instrumented: Broadcast[GCInstrumented],
                                         categoryInfo: Broadcast[RFCategoryInfo]) extends DataLoad[T, U] {
  override def loadFile(sc: SparkContext,
                        path: String,
                        numFeatures: Int,
                        minPartitions: Int): RDD[RawDataLabeled[T, U]] = {
    val parsed = parseARFFFile(sc, path, minPartitions)
    instrumented.value.gcALL

    parsed.map {
      case (label, values) =>
        RawDataLabeled(label, RawData.dense[T, U](values, typeInfo.value.NaN))
    }
  }

  def parseARFFFile(sc: SparkContext,
                    path: String,
                    minPartitions: Int): RDD[(Double, Array[T])] = {
    sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#") || line.startsWith("%") || line.startsWith("@")))
      .mapPartitions(it => {
        val toReturn = it.map(u => parseARFFRecord(u))
        instrumented.value.gc()
        toReturn
      })
  }

  def parseARFFRecord(line: String): (Double, Array[T]) = {
    val items = line.split(',')
    val label = Math.max(items.last.toDouble, 0)
    val values = items.dropRight(1).filter(_.nonEmpty).map(typeInfo.value.fromString)

    (label, values)
  }
}
