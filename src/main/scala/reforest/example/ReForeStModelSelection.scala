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

import reforest.ReForeStTrainerBuilder
import reforest.rf.feature.RFStrategyFeatureSQRT
import reforest.rf.parameter.{RFParameterBuilder, RFParameterType}
import reforest.util.{CCUtil, CCUtilIO}

/**
  * An example to use the ReForeSt library to perform Random Forest
  */
object ReForeStModelSelection {

  def main(args: Array[String]): Unit = {

    // Create the ReForeSt configuration.
    val property = RFParameterBuilder.apply
      .addParameter(RFParameterType.Dataset, "data/test10k-labels")
      .addParameter(RFParameterType.NumFeatures, 794)
      .addParameter(RFParameterType.NumClasses, 10)
      .addParameter(RFParameterType.ModelSelectionEpsilon, 0.001)
      .addParameter(RFParameterType.NumTrees, Array(50, 80, 100))
      .addParameter(RFParameterType.Depth, Array(5, 10))
      .addParameter(RFParameterType.FeatureMultiplierPerNode, Array(1d, 2d))
      .addParameter(RFParameterType.BinNumber, Array(8, 16, 32))
      .addParameter(RFParameterType.SparkMaster, "local[4]")
      .addParameter(RFParameterType.SparkCoresMax, 4)
      .addParameter(RFParameterType.SparkPartition, 4 * 4)
      .addParameter(RFParameterType.SparkExecutorMemory, "4096m")
      .addParameter(RFParameterType.SparkExecutorInstances, 1)
      .addParameter(RFParameterType.SLCActive, true)
      .addParameter(RFParameterType.ModelSelection, true)
//      .addParameter(RFParameterType.TestAll, true)
//      .addParameter(RFParameterType.LogStat, true)
      .addStrategyFeature(new RFStrategyFeatureSQRT(794))
      .build

    val sc = CCUtil.getSparkContext(property)
    sc.setLogLevel("error")

    // Create the Random Forest classifier.
    val timeStart = System.currentTimeMillis()
    val rfRunner = ReForeStTrainerBuilder.apply(property).build(sc)

    // Train a Random Forest model.
    val model = rfRunner.trainClassifier()
    val timeEnd = System.currentTimeMillis()

    // Evaluate model on test instances and compute test error
    val labelAndPreds = rfRunner.getDataLoader.getTestingData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / rfRunner.getDataLoader.getTestingData.count()

    println("TEST ACCURACY (feature " + model.getFeaturePerNode + ")(bin " + model.getBinNumber + ")(depth " + model.getDepth + ")(trees " + model.getNumTrees + ") = " + (1 - testErr) + " " + testErr)
    println("Time: " + (timeEnd - timeStart))
    CCUtilIO.logACCURACY(property, model, model.getDepth, (1 - testErr), (timeEnd - timeStart))
    rfRunner.sparkStop()
  }
}
