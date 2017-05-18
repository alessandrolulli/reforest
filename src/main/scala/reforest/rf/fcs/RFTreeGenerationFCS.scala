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

package randomForest.test.reforest.rf

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import reforest.TypeInfo
import reforest.data._
import reforest.dataTree.Forest
import reforest.rf.fcs._
import reforest.rf.split.RFSplitterManager
import reforest.rf.{RFProperty, RFStrategy, RFTreeGeneration}
import reforest.util._

import scala.collection.Map

class RFTreeGenerationFCS[T, U](@transient private val sc: SparkContext,
                                maxNodesConcurrent: Int,
                                property: Broadcast[RFProperty],
                                typeInfo: Broadcast[TypeInfo[T]],
                                typeInfoWorking: Broadcast[TypeInfo[U]],
                                strategy: Broadcast[RFStrategy[T, U]],
                                tree: RFTreeGeneration[T, U],
                                sampleSize: Long) extends Serializable {


//  def generateBloomFilter(it: Iterator[StaticData[U]],
//                          rootArray: Broadcast[Array[TreeNodeDetailed[T, U]]],
//                          depth: Int) = {
//    val expectedElement = (sampleSize / Math.pow(2, depth)).toInt
//    val bloomFilterMap: mutable.Map[(Int, Int), BloomFilterAL[Long]] = mutable.Map.empty
//    //    val bitVectorMap: mutable.Map[(Int, Int), UnsafeBitArray] = mutable.Map.empty
//
//    it.foreach(data => {
//      // i am assuming working data is the same in all the tree (no rotation allowed)
//      val pointSignature = data.getWorkingData(0) match {
//        case v: WorkingDataDense[U] => {
//          MurmurHash3.arrayHash(v.values)
//        }
//        case v: WorkingDataSparse[U] => {
//          MurmurHash3.arrayHash(v.values)
//        }
//      }
//
//      var treeId = 0
//      while (treeId < property.value.numTrees) {
//        val weight = data.getBagging(treeId)
//        if (weight > 0) {
//          val point = data.getWorkingData(treeId)
//          val nodeIdOption = rootArray.value(treeId).getCurrentNodeId(point, typeInfoWorking.value)
//          if (nodeIdOption.isDefined) {
//            var bloomFilter = bloomFilterMap.get((treeId, nodeIdOption.get))
//            if (bloomFilter.isEmpty) {
//              bloomFilter = Some(BloomFilterAL[Long](expectedElement, property.value.bloomFilterProbability))
//              bloomFilterMap.put((treeId, nodeIdOption.get), bloomFilter.get)
//            }
//            bloomFilter.get.add(pointSignature)
//          }
//        }
//        treeId += 1
//      }
//    })
//
//    bloomFilterMap.map(t => (t._1, t._2.bits)).iterator
//  }

  def findBestCutFCS(sc: SparkContext,
                     dataIndex: RDD[StaticData[U]],
                     util: CCUtil,
                     splitter: Broadcast[RFSplitterManager[T, U]],
                     featureSelected: Map[(Int, Int), Array[Int]],
                     forestArg: Forest[T, U],
                     depth: Int,
                     instrumented: Broadcast[GCInstrumented],
                     memoryUtil: MemoryUtil): Forest[T, U] = {

    println("STARTING CYCLE " + depth)
    if (featureSelected.isEmpty) {
      forestArg
    } else {
      var toReturn = forestArg

      val featureIdMap = sc.broadcast(featureSelected.map(t => (t._1, t._2.sorted)))

      val fcsExecutor = FCSExecutor.build(sc, typeInfo, typeInfoWorking, splitter, property, strategy, memoryUtil, featureIdMap, tree, sampleSize, depth)

      val iterationNumber = fcsExecutor.init(depth)
      var iteration = 0
      while (iteration < iterationNumber) {
        println("ITERATION: " + (iteration + 1) + "/" + iterationNumber)
        toReturn = fcsExecutor.executeIteration(toReturn, dataIndex, depth, iteration, iterationNumber)
        iteration += 1
      }

      toReturn
    }
  }
}