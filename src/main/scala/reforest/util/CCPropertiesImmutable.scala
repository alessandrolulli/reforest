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

package reforest.util

/**
  * An immutable representation of the configurations loaded from file
  * @param loader an utility to load additional custom properties
  * @param appName the application name
  * @param dataset the dataset to use
  * @param outputFile the output location
  * @param jarPath the path to the jar file
  * @param sparkMaster the URI of the Spark Master
  * @param sparkPartition the minimum number of partitions for the RDD
  * @param sparkExecutorMemory the amount of memory to use on each executor
  * @param sparkBlockManagerSlaveTimeoutMs
  * @param sparkCoresMax the amount of cores to use in the cluster
  * @param sparkShuffleManager
  * @param sparkCompressionCodec
  * @param sparkShuffleConsolidateFiles
  * @param sparkAkkaFrameSize
  * @param sparkDriverMaxResultSize
  * @param sparkExecutorInstances
  * @param separator
  * @param instrumented
  */
class CCPropertiesImmutable(val loader: CCProperties,
                            val appName: String,
                            val dataset: String,
                            val outputFile: String,
                            val jarPath: String,
                            val sparkMaster: String,
                            val sparkPartition: Int,
                            val sparkExecutorMemory: String,
                            val sparkBlockManagerSlaveTimeoutMs: String,
                            val sparkCoresMax: Int,
                            val sparkShuffleManager: String,
                            val sparkCompressionCodec: String,
                            val sparkShuffleConsolidateFiles: String,
                            val sparkAkkaFrameSize: String,
                            val sparkDriverMaxResultSize: String,
                            val sparkExecutorInstances: Int,
                            val separator: String,
                            val instrumented: Boolean) extends Serializable {

}