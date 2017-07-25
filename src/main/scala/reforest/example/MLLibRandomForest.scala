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
import reforest.util.CCUtil

import scala.util.Random

object MLLibRandomForest {
  def main(args: Array[String]): Unit = {

    val property = RFParameterBuilder.apply
      .addParameter(RFParameterType.Dataset, "data/sample-covtype.libsvm")
      .addParameter(RFParameterType.NumFeatures, 54)
      .addParameter(RFParameterType.NumClasses, 10)
      .addParameter(RFParameterType.NumTrees, 100)
      .addParameter(RFParameterType.Depth, Array(10))
      .addParameter(RFParameterType.BinNumber, Array(8))
      .addParameter(RFParameterType.SparkMaster, "local[4]")
      .addParameter(RFParameterType.SparkCoresMax, 4)
      .addParameter(RFParameterType.SparkPartition, 4*4)
      .addParameter(RFParameterType.SparkExecutorMemory, "4096m")
      .addParameter(RFParameterType.SparkExecutorInstances, 1)
      .build


    val sc = CCUtil.getSparkContext(property)
    sc.setLogLevel("error")

    val timeStart = System.currentTimeMillis()
    val data = MLUtils.loadLibSVMFile(sc, property.dataset, property.numFeatures, property.sparkCoresMax * 2)

    val splits = data.randomSplit(Array(0.6, 0.2, 0.2), 0)
    val (trainingData, testData) = (splits(0), splits(2))

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
    println("Time: "+(timeEnd-timeStart))
    println("Test Error = " + testErr)
    if (property.outputTree) {
      println("Learned classification forest model:\n" + model.toDebugString)
    }
  }
}
