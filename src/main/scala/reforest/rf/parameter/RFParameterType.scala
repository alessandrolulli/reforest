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

package reforest.rf.parameter


//var sparkPartition: Int = sparkCoresMax

class RFParameterType()

object RFParameterType extends Enumeration {

  case object NumFeatures extends RFParameterTypeInt(2)

  case object NumClasses extends RFParameterTypeInt(2)

  case object NumRotations extends RFParameterTypeInt(10)

  case object RotationRandomSeed extends RFParameterTypeInt(0)

  case object NumTrees extends RFParameterTypeArrayInt(Array(10))

  case object BinNumber extends RFParameterTypeArrayInt(Array(32))

  case object Depth extends RFParameterTypeArrayInt(Array(10))

  case object FeatureMultiplierPerNode extends RFParameterTypeArrayDouble(Array(1.0))

  case object Instrumented extends RFParameterTypeBoolean(false)

  case object LogStat extends RFParameterTypeBoolean(true)

  case object SkipAccuracy extends RFParameterTypeBoolean(false)

  case object PermitSparseWorkingData extends RFParameterTypeBoolean(false)

  case object OutputTree extends RFParameterTypeBoolean(false)

  case object SLCActive extends RFParameterTypeBoolean(false)

  case object SLCActiveForce extends RFParameterTypeBoolean(false)

  case object ModelSelection extends RFParameterTypeBoolean(false)

  case object Rotation extends RFParameterTypeBoolean(false)

  case object TestAll extends RFParameterTypeBoolean(false)

  case object PoissonMean extends RFParameterTypeDouble(1.0)

  case object SLCSafeMemoryMultiplier extends RFParameterTypeDouble(1.4)

  case object ModelSelectionEpsilon extends RFParameterTypeDouble(0.0)

  case object ModelSelectionEpsilonRemove extends RFParameterTypeDouble(1.0)

  case object SparkExecutorInstances extends RFParameterTypeInt(1)

  case object SparkCoresMax extends RFParameterTypeInt(1)

  case object SparkPartition extends RFParameterTypeInt(SparkCoresMax.defaultValue * 4)

  case object MaxNodesConcurrent extends RFParameterTypeInt(-1)

  case object SLCDepth extends RFParameterTypeInt(-1)

  case object SLCNodesPerCore extends RFParameterTypeInt(1)

  case object SLCCycleActivation extends RFParameterTypeInt(-1)

  case object AppName extends RFParameterTypeString("ReForeSt")

  case object SparkDriverMaxResultSize extends RFParameterTypeString("1g")

  case object SparkAkkaFrameSize extends RFParameterTypeString("100")

  case object SparkShuffleConsolidateFiles extends RFParameterTypeString("false")

  case object SparkCompressionCodec extends RFParameterTypeString("lz4")

  case object SparkShuffleManager extends RFParameterTypeString("SORT")

  case object SparkBlockManagerSlaveTimeoutMs extends RFParameterTypeString("500000")

  case object SparkExecutorMemory extends RFParameterTypeString("1024m")

  case object SparkMaster extends RFParameterTypeString("local[1]")

  case object SparkExecutorExtraClassPath extends RFParameterTypeString("")

  case object JarPath extends RFParameterTypeString("")

  case object Dataset extends RFParameterTypeString("")

  case object Category extends RFParameterTypeString("")

  case object FileType extends RFParameterTypeString("")

}

class RFParameterTypeArrayInt(val defaultValue: Array[Int]) extends RFParameterType {
}

class RFParameterTypeArrayDouble(val defaultValue: Array[Double]) extends RFParameterType {
}

class RFParameterTypeInt(val defaultValue: Int) extends RFParameterType {
}

class RFParameterTypeBoolean(val defaultValue: Boolean) extends RFParameterType {
}

class RFParameterTypeString(val defaultValue: String) extends RFParameterType {
}

class RFParameterTypeDouble(val defaultValue: Double) extends RFParameterType {
}


