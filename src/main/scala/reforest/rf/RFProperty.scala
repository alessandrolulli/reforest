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

import org.apache.spark.storage.StorageLevel
import reforest.util.CCPropertiesImmutable

class RFProperty(val property : CCPropertiesImmutable) extends Serializable {
  val storageLevel = StorageLevel.MEMORY_AND_DISK

  val binNumber = property.loader.getInt("binNumber", 32)
  val numTrees = property.loader.getInt("numTrees", 3)
  val maxDepth = property.loader.getInt("maxDepth", 3)
  val numClasses = property.loader.getInt("numClasses", 2)
  val featureNumber = property.loader.getInt("numFeatures", 0)
  val poissonMean = property.loader.getDouble("poissonMean", 1.0)
  val fast = property.loader.getBoolean("fast", false)
  val fcsActive = property.loader.getBoolean("fcsActive", false)
  val fcsActiveSize = property.loader.getInt("fcsActiveSize", 1000)
  val fcsMinDepth = property.loader.getInt("fcsMinDepth", 2)
  val permitSparseWorkingData = property.loader.getBoolean("permitSparseWorkingData", false)
  val outputTree = property.loader.getBoolean("outputTree", false)
  val maxNodesConcurrent = property.loader.getInt("maxNodesConcurrent", -1)

  val util = property.util

  // ROTATION
  val numRotation = property.loader.getInt("numRotation", numTrees)
  val numMacroIteration = property.loader.getInt("numMacroIteration", 1)
}
