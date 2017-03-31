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
import reforest.{TypeInfo, TypeInfoByte, TypeInfoDouble}
import reforest.data.{RawDataLabeled, StaticData}
import reforest.dataTree.{Cut, TreeNode}
import reforest.rf.split.{RFSplit, RFSplitterManager}
import reforest.util._

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class RFAllInRunner[T: ClassTag, U: ClassTag](@transient val sc: SparkContext,
                                              val property: RFProperty,
                                              val instrumented: Broadcast[GCInstrumented],
                                              val strategy: RFStrategy[T, U],
                                              val typeInfo: TypeInfo[T],
                                              val typeInfoWorking: TypeInfo[U],
                                              val categoricalFeaturesInfo: RFCategoryInfo = new RFCategoryInfoEmpty) extends Serializable {


  val propertyBC = sc.broadcast(property)
  var testData: Option[RDD[RawDataLabeled[T, U]]] = Option.empty
  var workingData: Option[RDD[StaticData[U]]] = Option.empty

  var rootArray: Array[TreeNode[T, U]] = Array()
  var featureInfo: Option[Broadcast[RFSplitterManager[T, U]]] = Option.empty
  var memoryUtil : Option[MemoryUtil] = Option.empty
  val categoricalFeaturesInfoBC = sc.broadcast(categoricalFeaturesInfo)

  val typeInfoBC = sc.broadcast(typeInfo)
  val typeInfoWorkingBC = sc.broadcast(typeInfoWorking)
  val strategyBC = sc.broadcast(strategy)

  val svm = new SVMUtilImpl[T, U](typeInfoBC, instrumented, categoricalFeaturesInfoBC)
  val dataPrepare = new RFDataPrepare[T, U](typeInfoBC, instrumented, strategyBC, property.permitSparseWorkingData, property.poissonMean)
  val rfSplit = new RFSplit[T, U](typeInfoBC, typeInfoWorkingBC, instrumented, categoricalFeaturesInfoBC)

  def executeGCinstrumented(): Unit = {
    instrumented.value.gcALL()
  }

  def workingDataUnpersist() = {
    if (workingData.isDefined) workingData.get.unpersist()
  }

  def getTrainingData(rawData: RDD[RawDataLabeled[T, U]], featureSQRT: collection.mutable.Map[(Int, Int), Array[Int]], numTrees: Int = property.numTrees, macroIteration : Int = 0) = {
    property.util.io.logTIME(property.property.appName, "START-PREPARE")
    instrumented.value.gcALL()

    if(featureInfo.isEmpty) {
      val zzz = strategyBC.value.findSplits(rawData, typeInfoBC, typeInfoWorkingBC, instrumented, categoricalFeaturesInfoBC)
      featureInfo = Some(sc.broadcast(zzz._1))
      memoryUtil = Some(zzz._2)
    }

    val toReturn = dataPrepare.prepareData(rawData, featureInfo.get, property.featureNumber, memoryUtil.get, numTrees, macroIteration)
    instrumented.value.gcALL()

    for (i <- 0 to (numTrees - 1)) featureSQRT((i, 1)) = strategyBC.value.getSQRTFeatures()

    (toReturn, memoryUtil)
  }

  def getTestData() = {
    testData.get
  }

  def printTree(oracle: Array[TreeNode[T, U]] = rootArray) = {
    if (property.outputTree) {
      for (r <- oracle.zipWithIndex) {
        property.util.io.log(r._2.toString)
        property.util.io.log(r._1.toString)
        println(r._2.toString)
        println(r._1.toString)
      }
    }
  }

  def getTestError(skipAccuracy: Boolean, oracle: Array[TreeNode[T, U]] = rootArray) = {
    var testErr = -1d
    if (!skipAccuracy) {
      val rootArrayBC = sc.broadcast(oracle)
      val dataToTest = getTestData()
      val labelAndPreds = dataToTest.map { point =>
        val prediction = rootArrayBC.value.map(t => t.predict(point, typeInfo)).groupBy(identity).mapValues(_.size).maxBy(_._2)
        (point.label, prediction._1)
      }

      testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / dataToTest.count()
    }
    testErr
  }

  def loadData(splitSize: Double) = {
    val data = svm.loadLibSVMFile(sc, property.property.dataset, property.featureNumber, property.property.sparkCoresMax * 2)
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

    property.util.io.printToFile("stats.txt", property.property.appName, property.property.dataset,
      "numTrees", property.numTrees.toString,
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

    new RFModelStandard[T, U](sc.broadcast(rootArray), typeInfoBC)
  }

  def updateForest(tree: RFTreeGeneration[T, U], treeId: Int, nodeId: Int, cut: Cut[T, U], rootArray: Array[TreeNode[T, U]], featureSQRT: collection.mutable.Map[(Int, Int), Array[Int]]) = {
    tree.updateForest(cut, treeId, TreeNode.getNode(nodeId, rootArray(treeId)), featureSQRT)
  }

  // TODO the following code is really ugly... but for now it works
  def run(trainingDataRaw: RDD[RawDataLabeled[T, U]], numTrees: Int = property.numTrees, macroIteration : Int = 0) = {
    println("numTrees " + numTrees)
    rootArray = Array()
    for (i <- 1 to numTrees) rootArray = rootArray :+ TreeNode.emptyNode[T, U](1)

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
    val tree = new RFTreeGeneration[T, U](sc,
      propertyBC,
      typeInfoBC,
      typeInfoWorkingBC,
      categoricalFeaturesInfoBC,
      featureInfo.get,
      strategyBC,
      numTrees,
      macroIteration)

    property.util.io.logTIME(property.property.appName, "START-TREE")

    def averageSampleSizeValid = {
      //      println("AVERAGE NODE SIZE: " + incrementalMean.getMean)
      !property.fcsActive || incrementalMean.getMean > property.fcsActiveSize || !incrementalMean.isStarted || (property.maxDepth - depth) <= property.fcsMinDepth
    }

    while (depth < property.maxDepth && averageSampleSizeValid) {
      val timeCycleStartALL = System.currentTimeMillis()
      depth = depth + 1
      println("DEPTH: " + depth)
      incrementalMean.reset()

      ////////////////////////////////
      val bestCutArray = tree.findBestCut(sc, workingData.get, featureSQRT, rootArray, depth, instrumented)
      ////////////////////////////////
      val timeCycleStart = System.currentTimeMillis()

      if (!bestCutArray.isEmpty) {
        featureSQRT.clear

        bestCutArray.foreach(cutInfo => {
          val treeId = cutInfo._1._1
          val nodeId = cutInfo._1._2
          val cut = cutInfo._2

          incrementalMean.updateMean(updateForest(tree, treeId, nodeId, cut, rootArray, featureSQRT))
        })
      }
      val timeCycleEnd = System.currentTimeMillis()
      notConcurrentTime += (timeCycleEnd - timeCycleStart)
      cycleTimeList += (timeCycleStart - timeCycleStartALL).toString
    }
    if (depth < property.maxDepth && property.fcsActive) {
      val treeFCS = new RFTreeGenerationFCS[T, U](sc, property.binNumber, maxNodesConcurrent, typeInfoBC, sc.broadcast(typeInfoWorking), strategyBC, property.featureNumber, tree)
      println("SWITCHING TO FCS")
      val t0 = System.currentTimeMillis()
      rootArray = treeFCS.findBestCutFCS(sc, workingData.get, property.util, featureInfo.get, featureSQRT, rootArray, depth, property.maxDepth, property.numClasses, property.property.appName, instrumented, memoryUtil.get)
      val t1 = System.currentTimeMillis()
      cycleTimeList += (t1 - t0).toString
    }

    workingDataUnpersist
    (notConcurrentTime, timePreparationEND, timePreparationSTART, cycleTimeList, maxNodesConcurrent, memoryUtil.get.maximumConcurrentNumberOfFeature)
  }
}

object RFAllInRunner {
  def apply(sc: SparkContext,
            property: RFProperty) = new RFAllInRunner[Double, Byte](sc, property, sc.broadcast(new GCInstrumentedEmpty), new RFStrategyStandard(property), new TypeInfoDouble(), new TypeInfoByte())

  def apply[U: ClassTag](sc: SparkContext,
                         property: RFProperty,
                         typeInfoWorking: TypeInfo[U]) = new RFAllInRunner[Double, U](sc, property, sc.broadcast(new GCInstrumentedEmpty), new RFStrategyStandard(property), new TypeInfoDouble(), typeInfoWorking)

  def apply[T: ClassTag,
  U: ClassTag](sc: SparkContext,
               property: RFProperty,
               typeInfo: TypeInfo[T],
               typeInfoWorking: TypeInfo[U]) = new RFAllInRunner[T, U](sc, property, sc.broadcast(new GCInstrumentedEmpty), new RFStrategyStandard(property), typeInfo, typeInfoWorking)
}