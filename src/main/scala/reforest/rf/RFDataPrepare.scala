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
import org.apache.spark.rdd.RDD
import reforest.TypeInfo

import reforest.data.{RawDataLabeled, StaticData}
import reforest.rf.split.RFSplitterManager
import reforest.util.{GCInstrumented, MemoryUtil}

class RFDataPrepare[T, U](typeInfo: Broadcast[TypeInfo[T]],
                          instrumented: Broadcast[GCInstrumented],
                          strategy: Broadcast[RFStrategy[T, U]],
                          permitSparseWorkingData: Boolean,
                          poissonMean: Double) extends Serializable {

  def prepareData(dataIndex: RDD[RawDataLabeled[T, U]],
                  splitter: Broadcast[RFSplitterManager[T, U]],
                  featureNumber: Int,
                  memoryUtil: MemoryUtil,
                  numTrees: Int,
                  macroIteration : Int):
  RDD[StaticData[U]] = {

    dataIndex.mapPartitionsWithIndex { (partitionIndex, instances) =>

      strategy.value.prepareData(numTrees, macroIteration, splitter.value, partitionIndex, instances, instrumented.value, memoryUtil)
    }
  }
}
