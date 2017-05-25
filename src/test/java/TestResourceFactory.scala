import org.apache.spark.broadcast.Broadcast
import reforest.rf.split.RFSplitterSimpleRandom
import reforest.rf.{RFCategoryInfo, RFCategoryInfoEmpty}
import reforest.{TypeInfo, TypeInfoByte, TypeInfoDouble}

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

object TestResourceFactory {

//  def getBroadcastTypeInfoByte: Broadcast[TypeInfo[Byte]] = new SparkBroadcast[TypeInfo[Byte]](new TypeInfoByte)
//
//  def getBroadcastTypeInfoDouble: Broadcast[TypeInfo[Double]] = new SparkBroadcast[TypeInfo[Double]](new TypeInfoDouble)

  def getCategoricalInfo: RFCategoryInfo = new RFCategoryInfoEmpty

  def getSplitterRandom[T, U](min: T,
                              max: T,
                              typeInfo: TypeInfo[T],
                              typeInfoWorking: TypeInfo[U],
                              numberBin: Int) = new RFSplitterSimpleRandom[T, U](min, max,
    typeInfo,
    typeInfoWorking,
    numberBin,
    getCategoricalInfo)

  def getSplitterRandomDefault(min: Double,
                               max: Double,
                               numberBin : Int) = getSplitterRandom[Double, Byte](min, max, new TypeInfoDouble, new TypeInfoByte, numberBin)
}
