///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package reforest
//
//import org.apache.spark.mllib.tree.RandomForestInstrumented
//import org.apache.spark.mllib.tree.configuration.{Algo, QuantileStrategy, Strategy}
//import org.apache.spark.mllib.tree.impurity.Entropy
//import org.apache.spark.mllib.util.MLUtilsInstrumented
//import reforest.rf.RFProperty
//import reforest.util._
//
//import scala.util.Random
//
//object RandomForestInstrumentedExample
//{
//  def main(args: Array[String]): Unit = {
//
//    val property = new RFProperty(new CCProperties("RANDOM-FOREST-MLLIB", args(0)).load().getImmutable)
//
//    val util = new CCUtil(property)
//    val utilIO = new CCUtilIO(property)
//    utilIO.logTIME(property.appName, "START")
//
//    val sc = util.getSparkContext()
//    sc.setLogLevel(property.loader.get("logLevel", "error"))
//
//    val instrumented = sc.broadcast(property.property.instrumented match {
//      case true => new GCInstrumentedFull(sc)
//      case false => new GCInstrumentedEmpty
//    })
//
//    utilIO.logTIME(property.appName, "START-PREPARE")
//
//    val timeStart = System.currentTimeMillis()
//    // $example on$
//    // Load and parse the data file.
//    val data = MLUtilsInstrumented.loadLibSVMFile(sc, property.property.dataset, property.loader.getInt("numFeatures",0))
//    val t0 = System.currentTimeMillis()
//    // Split the data into training and test sets (30% held out for testing)
//    val splits = data.randomSplit(Array(0.7, 0.3), 0)
//    val (trainingData, testData) = (splits(0), splits(1))
//
//    // Train a RandomForest model.
//    // Empty categoricalFeaturesInfo indicates all features are continuous.
//    val numClasses = property.loader.getInt("numClasses", 3)
//    val categoricalFeaturesInfo = Map[Int, Int]()
//    val featureSubsetStrategy = "auto" // Let the algorithm choose.
//    val impurity = "entropy"
//    val skipAccuracy = property.loader.getBoolean("skipAccuracy", true)
//    val numTrees = property.loader.getInt("numTrees", 3)
//    val maxDepth = property.loader.getInt("maxDepth", 3)
//    val maxMemoryInMB = property.loader.getInt("maxMemoryInMB", 256)
//    val maxBins = 32
//
//    val s = new
//        Strategy(Algo.Classification, Entropy, maxDepth, numClasses, maxBins, QuantileStrategy.Sort, categoricalFeaturesInfo, 1, 0.0, maxMemoryInMB)
//
////    val model = RandomForest.trainClassifier(data, numClasses, categoricalFeaturesInfo,
////      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
//    utilIO.logTIME(property.appName, "START-TREE")
//    instrumented.value.start()
//    val model = RandomForestInstrumented.trainClassifier(trainingData, s, numTrees, featureSubsetStrategy, Random.nextInt())
//    instrumented.value.stop()
//
//    val timeEnd = System.currentTimeMillis()
//    utilIO.logTIME(property.appName, "START-ACCURACY")
//    // Evaluate model on test instances and compute test error
//
//
//    var testErr = -1d
//    if(!skipAccuracy) {
//      val labelAndPreds = testData.map { point =>
//        val prediction = model.predict(point.features)
//        (point.label, prediction)
//      }
//
//      testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
//      println("Test Error = " + testErr)
//      println("Learned classification forest model:\n" + model.toDebugString)
//    }
//
//
//    utilIO.printToFile("stats.txt", "RANDOM-FOREST-MLLIB", property.property.dataset,
//      "numTrees", numTrees.toString,
//      "maxDepth", maxDepth.toString,
//      "timeALL", (timeEnd - timeStart).toString,
//      "timePREPARATION", (t0 - timeStart).toString,
//      "testError", testErr.toString,
//      "sparkCoresMax", property.property.sparkCoresMax.toString,
//      "sparkExecutorInstances", property.property.sparkExecutorInstances.toString)
//    utilIO.logTIME(property.appName, "STOP")
////    "Learned classification forest model", model.toDebugString)
//
//    // Save and load model
//    //model.save(sc, property.outputFile)
////    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
//    // $example off$
//  }
//
//}
