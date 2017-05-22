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
import org.apache.spark.rdd.RDD
import reforest.data.RawDataLabeled

/**
  * An utility to load data from different file formats in raw data labeled
  * @tparam T raw data type
  * @tparam U working data type
  */
trait DataLoad[T, U] extends Serializable {

  /**
    * Load the data from a file
    * @param sc the Spark Context
    * @param path the file path
    * @param numFeatures the number of features in the dataset
    * @param minPartitions the minimum number of partition of the RDD
    * @return the loaded dataset in RawDataLabeled format
    */
  def loadFile(sc: SparkContext,
               path: String,
               numFeatures: Int,
               minPartitions: Int): RDD[RawDataLabeled[T, U]]
}
