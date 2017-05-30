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

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import randomForest.test.reforest.rf.RFTreeGenerationFCS
import reforest.data.load.{ARFFUtil, LibSVMUtil}
import reforest.data.{RawDataLabeled, StaticData}
import reforest.dataTree._
import reforest.rf.split.RFSplitterManager
import reforest.util._
import reforest.{TypeInfo, TypeInfoByte, TypeInfoDouble}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class RFRunner[T: ClassTag, U: ClassTag](@transient private val sc: SparkContext,
                                         val property: RFProperty,
                                         val instrumented: Broadcast[GCInstrumented],
                                         val strategy: RFStrategy[T, U],
                                         val typeInfo: TypeInfo[T],
                                         val typeInfoWorking: TypeInfo[U],
                                         val categoricalFeaturesInfo: RFCategoryInfo = new RFCategoryInfoEmpty) extends Serializable {


  val propertyBC = sc.broadcast(property)
  var testData: Option[RDD[RawDataLabeled[T, U]]] = Option.empty
  var workingData: Option[RDD[StaticData[U]]] = Option.empty

  var forest: Forest[T, U] = new ForestIncremental[T, U](property.numTrees, property.maxDepth)
  var featureInfo: Option[Broadcast[RFSplitterManager[T, U]]] = Option.empty
  var memoryUtil: Option[MemoryUtil] = Option.empty
  val categoricalFeaturesInfoBC = sc.broadcast(categoricalFeaturesInfo)

  val typeInfoBC = sc.broadcast(typeInfo)
  val typeInfoWorkingBC = sc.broadcast(typeInfoWorking)
  val strategyBC = sc.broadcast(strategy)

  val svm = CCUtil.getDataLoader[T, U](property, typeInfoBC, instrumented, categoricalFeaturesInfoBC)
  val dataPrepare = new RFDataPrepare[T, U](typeInfoBC, instrumented, strategyBC, property.permitSparseWorkingData, property.poissonMean)

  var trainingTime = 0l

  def getTrainingTime = {
    trainingTime
  }

  def sparkStop() = sc.stop()

  def executeGCinstrumented(): Unit = {
    instrumented.value.gcALL()
  }

  def workingDataUnpersist() = {
    if (workingData.isDefined) workingData.get.unpersist()
  }

  def getTrainingData(rawData: RDD[RawDataLabeled[T, U]], featureSQRT: collection.mutable.Map[(Int, Int), Array[Int]], numTrees: Int = property.numTrees, macroIteration: Int = 0) = {
    CCUtilIO.logTIME(property, property.appName, "START-PREPARE")
    instrumented.value.gcALL()

    if (featureInfo.isEmpty) {
      val zzz = strategyBC.value.findSplits(rawData, typeInfoBC, typeInfoWorkingBC, instrumented, categoricalFeaturesInfoBC)
      featureInfo = Some(sc.broadcast(zzz._1))
      memoryUtil = Some(zzz._2)
    }

    val toReturn = dataPrepare.prepareData(rawData, featureInfo.get, property.featureNumber, memoryUtil.get, numTrees, macroIteration)
    instrumented.value.gcALL()

    for (i <- 0 to (numTrees - 1)) featureSQRT((i, 0)) = strategyBC.value.getStrategyFeature().getFeaturePerNode

    (toReturn, memoryUtil)
  }

  def getTestData() = {
    testData.get
  }

  def printTree(oracle: Forest[T, U] = forest) = {
    if (property.outputTree) {
      println(oracle.toString())
    }
  }

  def loadData(splitSize: Double) = {
    val data = svm.loadFile(sc, property.dataset, property.featureNumber, property.sparkCoresMax * 2)
    instrumented.value.gcALL
    val splits = data.randomSplit(Array(splitSize, (1 - splitSize)), 0)
    val (trainingData, testDataToSet) = (splits(0), splits(1))

    testData = Some(testDataToSet)
    trainingData
  }

  def trainClassifier(trainingData: RDD[RawDataLabeled[T, U]]): RFModel[T, U] = {
    val t0 = System.currentTimeMillis()
    val (notConcurrentTime, timePreparationEND, timePreparationSTART, cycleTimeList, maxNodesConcurrent, featurePerIteration) = run(trainingData)
    val t1 = System.currentTimeMillis()
    println("Time: " + (t1 - t0))

    trainingTime = (t1 - t0)

    CCUtilIO.printToFile(property, "stats.txt", property.appName, property.dataset,
      "timeALL", (t1 - t0).toString,
      "notConcurrentTime", notConcurrentTime.toString,
      "preparationTime", (timePreparationEND - timePreparationSTART).toString,
      "cycleTime", cycleTimeList.mkString("|"),
      "maxNodesConcurrent", maxNodesConcurrent.toString,
      "maxFeaturePerIteration", featurePerIteration.toString
    )

    new RFModelStandard[T, U](sc.broadcast(forest), typeInfoBC, property.numClasses)
  }

  def updateForest(tree: RFTreeGeneration[T, U], treeId: Int, nodeId: Int, cut: CutDetailed[T, U], forest: Forest[T, U], featureSQRT: collection.mutable.Map[(Int, Int), Array[Int]]) = {
    tree.updateForest(cut, treeId, nodeId, forest, Some(featureSQRT))
  }

  // TODO the following code is really ugly... but for now it works
  def run(trainingDataRaw: RDD[RawDataLabeled[T, U]], numTrees: Int = property.numTrees, macroIteration: Int = 0) = {
    println("numTrees " + numTrees)
    forest = new ForestFull[T, U](numTrees, property.maxDepth)

    instrumented.value.gcALL

    if (property.fast) trainingDataRaw.persist(property.storageLevel)

    val featureSQRT = collection.mutable.Map[(Int, Int), Array[Int]]().withDefaultValue(Array())

    val timePreparationSTART = System.currentTimeMillis()
    val trainingDataTmp = getTrainingData(trainingDataRaw, featureSQRT, numTrees, macroIteration)
    workingData = Some(trainingDataTmp._1)
    val memoryUtil = trainingDataTmp._2

    instrumented.value.start()
    val dataSize = workingData.get.persist(property.storageLevel).count()
    instrumented.value.stop()

    trainingDataRaw.unpersist()
    instrumented.value.gcALL()

    val timePreparationEND = System.currentTimeMillis()

    var depth = 0
    var notConcurrentTime = 0l
    var cycleTimeList = new ListBuffer[String]()
    val incrementalMean = new IncrementalMean

    val maxNodesConcurrentConfig = property.maxNodesConcurrent
    val maxNodesConcurrent = if (maxNodesConcurrentConfig > 0) maxNodesConcurrentConfig else memoryUtil.get.maximumConcurrentNodes
    property.maxNodesConcurrent = maxNodesConcurrent
    val tree = new RFTreeGeneration[T, U](sc,
      propertyBC,
      typeInfoBC,
      typeInfoWorkingBC,
      categoricalFeaturesInfoBC,
      featureInfo.get,
      strategyBC,
      numTrees,
      macroIteration)

    CCUtilIO.logTIME(property, property.appName, "START-TREE")

    def switchToFCS(nodeNumber: Int) = {
      //      memoryUtil.get.switchToFCS(depth, featureSQRT.size)

      if (property.fcsActive && property.fcsDepth != -1) {
        property.fcsDepth == depth
      } else {
        if(nodeNumber < property.sparkCoresMax) {
          false
        } else {
          property.fcsActive && memoryUtil.get.switchToFCS(depth, featureSQRT.size)
        }
      }
    }

    while (depth < property.maxDepth && !switchToFCS(featureSQRT.size)) {
      val timeCycleStartALL = System.currentTimeMillis()
      depth = depth + 1
      println("DEPTH: " + depth)
      incrementalMean.reset()

      ////////////////////////////////
      val bestCutArray = tree.findBestCut(sc, workingData.get, featureSQRT, forest, depth, instrumented)
      ////////////////////////////////
      val timeCycleStart = System.currentTimeMillis()

      if (!bestCutArray.isEmpty) {
        featureSQRT.clear

        bestCutArray.foreach(cutInfo => {
          val treeId = cutInfo._1._1
          val nodeId = cutInfo._1._2
          val cut = cutInfo._2

          incrementalMean.updateMean(updateForest(tree, treeId, nodeId, cut, forest, featureSQRT))
        })
      }
      val timeCycleEnd = System.currentTimeMillis()
      notConcurrentTime += (timeCycleEnd - timeCycleStart)
      cycleTimeList += (timeCycleStart - timeCycleStartALL).toString
    }
    if (depth < property.maxDepth && property.fcsActive) {
      val treeFCS = new RFTreeGenerationFCS[T, U](sc, maxNodesConcurrent, propertyBC, typeInfoBC, sc.broadcast(typeInfoWorking), strategyBC, tree, strategy.getSampleSize)
      println("SWITCHING TO FCS")
      val t0 = System.currentTimeMillis()
      forest = treeFCS.findBestCutFCS(sc, workingData.get, featureInfo.get, featureSQRT, forest, depth, instrumented, memoryUtil.get)
      val t1 = System.currentTimeMillis()
      cycleTimeList += (t1 - t0).toString
    }

    //    workingDataUnpersist
    (notConcurrentTime, timePreparationEND, timePreparationSTART, cycleTimeList, maxNodesConcurrent, memoryUtil.get.maximumConcurrentNumberOfFeature)
  }
}

object RFRunner {
  def apply(property: RFProperty) = {
    val sc = CCUtil.getSparkContext(property)
    val strategyFeature = property.strategyFeature
    new RFRunner[Double, Byte](sc, property, sc.broadcast(new GCInstrumentedEmpty), new RFStrategyStandard(property, strategyFeature), new TypeInfoDouble(), new TypeInfoByte())
  }

  def apply[T: ClassTag, U: ClassTag](property: RFProperty, typeInfoRawData: TypeInfo[T], typeInfoWorkingData: TypeInfo[U]) = {
    val sc = CCUtil.getSparkContext(property)
    val strategyFeature = property.strategyFeature
    new RFRunner[T, U](sc, property, sc.broadcast(new GCInstrumentedEmpty), new RFStrategyStandard(property, strategyFeature), typeInfoRawData, typeInfoWorkingData)
  }

  def apply(sc: SparkContext,
            property: RFProperty,
            strategyFeature: RFStrategyFeature) = new RFRunner[Double, Byte](sc, property, sc.broadcast(new GCInstrumentedEmpty), new RFStrategyStandard(property, strategyFeature), new TypeInfoDouble(), new TypeInfoByte())

  def apply[U: ClassTag](sc: SparkContext,
                         property: RFProperty,
                         strategyFeature: RFStrategyFeature,
                         typeInfoWorking: TypeInfo[U],
                         categoricalFeaturesInfo: RFCategoryInfo = new RFCategoryInfoEmpty) = new RFRunner[Double, U](sc, property, sc.broadcast(new GCInstrumentedEmpty), new RFStrategyStandard(property, strategyFeature), new TypeInfoDouble(), typeInfoWorking)

  def apply[T: ClassTag,
  U: ClassTag](sc: SparkContext,
               property: RFProperty,
               strategyFeature: RFStrategyFeature,
               typeInfo: TypeInfo[T],
               typeInfoWorking: TypeInfo[U]) = new RFRunner[T, U](sc, property, sc.broadcast(new GCInstrumentedEmpty), new RFStrategyStandard(property, strategyFeature), typeInfo, typeInfoWorking)
}