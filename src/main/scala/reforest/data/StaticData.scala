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

trait StaticData[U] extends Serializable {
  def getLabel : Byte
  def getWorkingData(treeId : Int) : WorkingData[U]
  def getBagging(treeId : Int) : Int
  def getBagging() : Array[Byte]
}

class StaticDataClassic[U](label : Byte, workingData : WorkingData[U], bagging : Array[Byte]) extends StaticData[U] {
  def getLabel : Byte = label
  def getWorkingData(treeId : Int) : WorkingData[U] = workingData
  def getBagging(treeId : Int) = bagging(treeId)
  def getBagging() : Array[Byte] = bagging
}

class StaticDataRotation[U](label : Byte, workingData : Array[WorkingData[U]]) extends StaticData[U] {
  def getLabel : Byte = label
  def getWorkingData(treeId : Int) : WorkingData[U] = workingData(treeId)
  def getBagging(treeId : Int) = 1
  def getBagging() : Array[Byte] = Array.fill(workingData.length)(1)
}
