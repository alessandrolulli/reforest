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

package reforest.rf.slc

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import reforest.TypeInfo
import reforest.data._
import reforest.data.tree.ForestManager
import reforest.rf.feature.RFFeatureManager
import reforest.rf.parameter.RFParameter
import reforest.rf.{RFSkip, RFStrategy, RFTreeGeneration}
import reforest.util._

class SLCTreeGeneration[T, U](@transient private val sc: SparkContext,
                              property: Broadcast[RFParameter],
                              typeInfo: Broadcast[TypeInfo[T]],
                              typeInfoWorking: Broadcast[TypeInfo[U]],
                              sampleSize: Long) extends Serializable {

  var fcsExecutor : Option[SLCExecutor[T, U]] = Option.empty

  def findBestCutSLC(dataIndex: RDD[StaticData[U]],
                     forestManager: ForestManager[T, U],
                     featureManager: RFFeatureManager,
                     depthToStop : Int,
                     instrumented: Broadcast[GCInstrumented],
                    skip : RFSkip): ForestManager[T, U] = {

    if (featureManager.getActiveNodesNum <= 0) {
      forestManager
    } else {
      var toReturn = forestManager

      val splitterManagerBC = sc.broadcast(forestManager.splitterManager)

      if(fcsExecutor.isEmpty) {
        fcsExecutor = Some(SLCExecutor.build(sc, typeInfo, typeInfoWorking, property,
          splitterManagerBC, sampleSize))
      }

      toReturn = fcsExecutor.get.executeSLC(toReturn, featureManager, dataIndex, depthToStop, skip)

      splitterManagerBC.unpersist()

      toReturn
    }
  }
}