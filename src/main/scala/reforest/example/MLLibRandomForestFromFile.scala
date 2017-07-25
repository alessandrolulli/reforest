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
import reforest.rf.feature.RFStrategyFeatureSQRT
import reforest.rf.parameter._
import reforest.util.{CCUtil, CCUtilIO}

import scala.util.Random

object MLLibRandomForestFromFile {
  def main(args: Array[String]): Unit = {

    val property = RFParameterFromFile(args(0)).applyAppName("MLLib")

    val sc = CCUtil.getSparkContext(property)
    sc.setLogLevel("error")

    val timeStart = System.currentTimeMillis()
    val data = MLUtils.loadLibSVMFile(sc, property.dataset, property.numFeatures, property.sparkCoresMax * 2)

    val splits = data.randomSplit(Array(0.7, 0.3), 0)
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    //    val categoricalFeaturesInfo = Array.tabulate(200)(i => (i, 5)).toMap
    val categoricalFeaturesInfo = Map[Int, Int]()
    val featureSubsetStrategy = "sqrt"
    val impurity = "entropy"

    val s = new
        Strategy(Algo.Classification, Entropy, property.getMaxDepth, property.numClasses, property.getMaxBinNumber, QuantileStrategy.Sort, categoricalFeaturesInfo, 1)

    val model = RandomForest.trainClassifier(trainingData, s, property.getMaxNumTrees, featureSubsetStrategy, Random.nextInt())
    val timeEnd = System.currentTimeMillis()

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    CCUtilIO.logACCURACY(property, (1-testErr), (timeEnd-timeStart))
    println("Time: "+(timeEnd-timeStart))
    println("Test Error = " + testErr)
    if (property.outputTree) {
      println("Learned classification forest model:\n" + model.toDebugString)
    }
  }
}
