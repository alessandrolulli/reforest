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

package reforest.rf.rotation

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import reforest.data.RawDataLabeled
import reforest.{TypeInfo, TypeInfoByte, TypeInfoDouble}
import reforest.rf._
import reforest.util.{GCInstrumented, GCInstrumentedEmpty}

import scala.reflect.ClassTag

class RFAllInRunnerRotation[T: ClassTag, U: ClassTag](@transient override val sc: SparkContext,
                                                      override val property: RFProperty,
                                                      val dataUtil: RotationDataUtil[T, U],
                                                      override val instrumented: Broadcast[GCInstrumented],
                                                      override val typeInfo: TypeInfo[T],
                                                      override val typeInfoWorking: TypeInfo[U],
                                                      override val categoricalFeaturesInfo: RFCategoryInfo = new RFCategoryInfoEmpty)
  extends RFAllInRunner[T, U](sc, property, instrumented, new RFStrategyRotation(property, sc.broadcast(dataUtil.matrices)), typeInfo, typeInfoWorking, categoricalFeaturesInfo) {

  val numRotation = property.numRotation

  override def loadData(splitSize: Double) = {

    val (trainingData, testDataToSet) = dataUtil.getScaledData(splitSize, svm)

    testData = Some(testDataToSet)
    trainingData
  }

  override def trainClassifier(trainingData: RDD[RawDataLabeled[T, U]]) = {

    trainingData.persist(property.storageLevel)
    trainingData.count()

    val model = new RFModelAggregator[T, U](property.numClasses)

    require(property.numTrees % numRotation.toDouble == 0 && numRotation <= property.numTrees)
    require(property.numRotation % property.numMacroIteration.toDouble == 0)

    //TODO
    require(property.numRotation == property.numTrees)

    val numTreesPerIteration = property.numTrees / property.numMacroIteration
    for(i <- 0 to property.numMacroIteration - 1) {
      val t0 = System.currentTimeMillis()
      val (notConcurrentTime, timePreparationEND, timePreparationSTART, cycleTimeList, maxNodesConcurrent, featurePerIteration) = run(trainingData, numTreesPerIteration, i)
      workingDataUnpersist()

      var count = 0
      while (count < numTreesPerIteration) {
        model.addModel(new RFModelRotate[T, U](sc.broadcast(Array(rootArray(count))), typeInfoBC, sc.broadcast(dataUtil.matrices((i*numTreesPerIteration)+count))))
        count += 1
      }
      val t1 = System.currentTimeMillis()

      property.util.io.printToFile("stats.txt", property.property.appName, property.property.dataset,
        "numTrees", numTreesPerIteration.toString,
        "macroIteration", (i+1)+"/"+property.numMacroIteration,
        "numRotation", numRotation.toString,
        "numTreesTOT", property.numTrees.toString,
        "maxDepth", property.maxDepth.toString,
        "binNumber", property.binNumber.toString,
        "timeALL", (t1 - t0).toString,
        "notConcurrentTime", notConcurrentTime.toString,
        "preparationTime", (timePreparationEND - timePreparationSTART).toString,
        "cycleTime", cycleTimeList.mkString("|"),
        "maxNodesConcurrent", maxNodesConcurrent.toString,
        "maxFeaturePerIteration", featurePerIteration.toString,
        "sparkCoresMax", property.property.sparkCoresMax.toString,
        "sparkExecutorInstances", property.property.sparkExecutorInstances.toString
      )
    }

    model
  }
}

object RFAllInRunnerRotation {
  def apply(sc: SparkContext,
            property: RFProperty,
            dataUtil: RotationDataUtil[Double, Byte]) = new RFAllInRunnerRotation[Double, Byte](sc, property, dataUtil, sc.broadcast(new GCInstrumentedEmpty), new TypeInfoDouble(), new TypeInfoByte())

  def apply[U: ClassTag](sc: SparkContext,
                         property: RFProperty,
                         dataUtil: RotationDataUtil[Double, U],
                         typeInfoWorking: TypeInfo[U]) = new RFAllInRunnerRotation[Double, U](sc, property, dataUtil, sc.broadcast(new GCInstrumentedEmpty), new TypeInfoDouble(), typeInfoWorking)

  def apply[T: ClassTag,
  U: ClassTag](sc: SparkContext,
               property: RFProperty,
               dataUtil: RotationDataUtil[T, U],
               typeInfo: TypeInfo[T],
               typeInfoWorking: TypeInfo[U]) = new RFAllInRunnerRotation[T, U](sc, property, dataUtil, sc.broadcast(new GCInstrumentedEmpty), typeInfo, typeInfoWorking)
}