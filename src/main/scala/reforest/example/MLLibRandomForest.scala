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

package reforest.example

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.{Algo, QuantileStrategy, Strategy}
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.util.MLUtils
import reforest.rf.RFProperty
import reforest.util.{CCProperties, CCUtil, CCUtilIO}

import scala.util.Random

object MLLibRandomForest {
  def main(args: Array[String]): Unit = {

    val property = new RFProperty()
    property.appName = "RANDOM-FOREST-MLLIB"
    property.dataset = "data/sample-infimnist.libsvm"
    property.featureNumber = 794

    property.numTrees = 100
    property.maxDepth = 5
    property.numClasses = 10


    val sc = CCUtil.getSparkContext(property)

    CCUtilIO.logTIME(property, property.appName, "START-PREPARE")

    val timeStart = System.currentTimeMillis()
    val data = MLUtils.loadLibSVMFile(sc, property.dataset, property.featureNumber, property.sparkCoresMax * 2)
    val t0 = System.currentTimeMillis()

    val splits = data.randomSplit(Array(0.7, 0.3), 0)
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    val numClasses = property.numClasses
    //    val categoricalFeaturesInfo = Array.tabulate(200)(i => (i, 5)).toMap
    val categoricalFeaturesInfo = Map[Int, Int]()
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

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    if (property.outputTree) {
      println("Learned classification forest model:\n" + model.toDebugString)
    }

  }

}
