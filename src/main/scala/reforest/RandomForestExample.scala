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

package reforest

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.{Algo, QuantileStrategy, Strategy}
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.util.MLUtils
import reforest.rf.{RFCategoryInfoEmpty, RFCategoryInfoSpecialized, RFProperty, RFPropertyFile}
import reforest.util.{CCProperties, CCUtil, CCUtilIO}

import scala.util.Random

object RandomForestExample
{
  def main(args: Array[String]): Unit = {

    val property = new RFPropertyFile(new CCProperties("MLLib", args(0)).load().getImmutable)

    CCUtilIO.logTIME(property, property.appName, "START")

    val sc = CCUtil.getSparkContext(property)

    CCUtilIO.logTIME(property, property.appName, "START-PREPARE")

    val timeStart = System.currentTimeMillis()
    val data = MLUtils.loadLibSVMFile(sc, property.dataset, property.featureNumber, property.sparkCoresMax * 2)
    val t0 = System.currentTimeMillis()

    val splits = data.randomSplit(Array(0.7, 0.3), 0)
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    val numClasses = property.numClasses


    val categoricalFeaturesInfo = property.category match
    {
      case "" => Map[Int, Int]()
      case categoryValue : String => Array.tabulate(200)(i => (i, 5)).toMap
      case _ => Map[Int, Int]()
    }
    val featureSubsetStrategy = "sqrt"
    val impurity = "entropy"
    val skipAccuracy = property.skipAccuracy
    val numTrees = property.numTrees
    val maxDepth = property.maxDepth
    val binNumber = property.binNumber

    val s = new
        Strategy(Algo.Classification, Entropy, maxDepth, numClasses, binNumber, QuantileStrategy.Sort, categoricalFeaturesInfo, 1)

    CCUtilIO.logTIME(property, property.appName, "START-TREE")
    val model = RandomForest.trainClassifier(trainingData, s, numTrees, featureSubsetStrategy, Random.nextInt())


    val timeEnd = System.currentTimeMillis()
    CCUtilIO.logTIME(property, property.appName, "START-ACCURACY")

    var testErr = -1d
    if(!skipAccuracy) {
      val labelAndPreds = testData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }

      testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
      println("Test Error = " + testErr)
      if(property.outputTree) {
        println("Learned classification forest model:\n" + model.toDebugString)
      }
    }

    CCUtilIO.printToFile(property, "stats.txt", "RANDOM-FOREST-MLLIB", property.dataset,
      "numTrees", numTrees.toString,
      "maxDepth", maxDepth.toString,
      "binNumber", binNumber.toString,
      "timeALL", (timeEnd - timeStart).toString,
      "timePREPARATION", (t0 - timeStart).toString,
      "testError", testErr.toString,
      "sparkCoresMax", property.sparkCoresMax.toString,
      "sparkExecutorInstances", property.sparkExecutorInstances.toString)
    CCUtilIO.logTIME(property, property.appName, "STOP")
//    "Learned classification forest model", model.toDebugString)

    // Save and load model
    //model.save(sc, property.outputFile)
//    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
    // $example off$
  }

}
