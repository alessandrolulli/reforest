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
import reforest.rf._
import reforest.util.{CCUtil, GCInstrumented, GCInstrumentedEmpty}
import reforest.{TypeInfo, TypeInfoByte, TypeInfoDouble}

import scala.reflect.ClassTag

/**
  * The ReForeSt executor for computing random rotations
  * @param sc the Spark Context
  * @param property the ReForeSt property
  * @param dataUtil the utility for random rotations
  * @param instrumented the instrumentation for the GC
  * @param typeInfo the type information for the raw data
  * @param typeInfoWorking the type information for the working data
  * @param strategy the startegy to compute the random forest
  * @param categoricalFeaturesInfo the information for the categorical features
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFRunnerRotation[T: ClassTag, U: ClassTag](@transient val sc: SparkContext,
                                                 override val property: RFProperty,
                                                 val dataUtil: RotationDataUtil[T, U],
                                                 override val instrumented: Broadcast[GCInstrumented],
                                                 override val typeInfo: TypeInfo[T],
                                                 override val typeInfoWorking: TypeInfo[U],
                                                 strategy: RFStrategy[T, U],
                                                 override val categoricalFeaturesInfo: RFCategoryInfo = new RFCategoryInfoEmpty)
  extends RFRunner[T, U](sc, property, instrumented, strategy, typeInfo, typeInfoWorking, categoricalFeaturesInfo) {

  private val numRotation = property.numRotation

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

    val numTreesPerIteration = property.numTrees / numRotation
    for (i <- 0 to numRotation - 1) {
      val t0 = System.currentTimeMillis()
      val (notConcurrentTime, timePreparationEND, timePreparationSTART, cycleTimeList, maxNodesConcurrent, featurePerIteration) = run(trainingData, numTreesPerIteration, i)
      workingDataUnpersist()
      model.addModel(new RFModelRotate[T, U](sc.broadcast(forest), typeInfoBC, sc.broadcast(dataUtil.matrices(property.rotationRandomSeed + i)), property.numClasses))
      val t1 = System.currentTimeMillis()

      trainingTime += (t1 - t0)

//      property.util.io.printToFile("stats.txt", property.appName, property.dataset,
//        "numTreesRotation", numTreesPerIteration.toString,
//        "numRotation", numRotation.toString,
//        "rotation", i.toString,
//        "timeALL", (t1 - t0).toString,
//        "notConcurrentTime", notConcurrentTime.toString,
//        "preparationTime", (timePreparationEND - timePreparationSTART).toString,
//        "cycleTime", cycleTimeList.mkString("|"),
//        "maxNodesConcurrent", maxNodesConcurrent.toString,
//        "maxFeaturePerIteration", featurePerIteration.toString
//      )
    }

    model
  }
}

/**
  * It constructs an executor of ReForeSt random rotations
  */
object RFRunnerRotation {
  def apply(property: RFProperty) = {
    val sc = CCUtil.getSparkContext(property)
    sc.setLogLevel("error")
    val dataUtil = new RotationDataUtil[Double, Byte](sc, property, sc.broadcast(new TypeInfoDouble), property.sparkCoresMax * 2)
    new RFRunnerRotation[Double, Byte](sc, property, dataUtil, sc.broadcast(new GCInstrumentedEmpty), new TypeInfoDouble(), new TypeInfoByte(), new RFStrategyRotation(property, property.strategyFeature, sc.broadcast(dataUtil.matrices)))
  }

  def apply[T: ClassTag, U: ClassTag](property: RFProperty, typeInfoRawData: TypeInfo[T], typeInfoWorkingData: TypeInfo[U]) = {
    val sc = CCUtil.getSparkContext(property)
    val strategyFeature = property.strategyFeature
    val dataUtil = new RotationDataUtil[T, U](sc, property, sc.broadcast(typeInfoRawData), property.sparkCoresMax * 2)
    new RFRunnerRotation[T, U](sc, property, dataUtil, sc.broadcast(new GCInstrumentedEmpty), typeInfoRawData, typeInfoWorkingData, new RFStrategyRotation(property, strategyFeature, sc.broadcast(dataUtil.matrices)))
  }

  def apply(sc: SparkContext,
            property: RFProperty,
            strategyFeature: RFStrategyFeature,
            dataUtil: RotationDataUtil[Double, Byte]) = new RFRunnerRotation[Double, Byte](sc, property, dataUtil, sc.broadcast(new GCInstrumentedEmpty), new TypeInfoDouble(), new TypeInfoByte(), new RFStrategyRotation(property, strategyFeature, sc.broadcast(dataUtil.matrices)))

  def apply[U: ClassTag](sc: SparkContext,
                         property: RFProperty,
                         strategyFeature: RFStrategyFeature,
                         dataUtil: RotationDataUtil[Double, U],
                         typeInfoWorking: TypeInfo[U]) = new RFRunnerRotation[Double, U](sc, property, dataUtil, sc.broadcast(new GCInstrumentedEmpty), new TypeInfoDouble(), typeInfoWorking, new RFStrategyRotation(property, strategyFeature, sc.broadcast(dataUtil.matrices)))

  def apply[T: ClassTag,
  U: ClassTag](sc: SparkContext,
               property: RFProperty,
               strategyFeature: RFStrategyFeature,
               dataUtil: RotationDataUtil[T, U],
               typeInfo: TypeInfo[T],
               typeInfoWorking: TypeInfo[U]) = new RFRunnerRotation[T, U](sc, property, dataUtil, sc.broadcast(new GCInstrumentedEmpty), typeInfo, typeInfoWorking, new RFStrategyRotation(property, strategyFeature, sc.broadcast(dataUtil.matrices)))
}