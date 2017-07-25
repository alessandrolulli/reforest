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
import reforest.rf.parameter.{RFParameterBuilder, RFParameterFromFile, RFParameterType}
import reforest.util.{CCProperties, CCUtil, CCUtilIO}

/**
  * An example to use the ReForeSt library to perform Random Forest
  */
object ReForeStFromFile {

  def main(args: Array[String]): Unit = {

    val property = RFParameterFromFile(args(0))

    val sc = CCUtil.getSparkContext(property)

    val rfTrainer = ReForeStTrainerBuilder.apply()
      .addProperty(property)

    // Create the Random Forest classifier.
    sc.setLogLevel("error")
    val timeStart = System.currentTimeMillis()
    val rfRunner = rfTrainer.build(sc)

    // Train a Random Forest model.
    val model = rfRunner.trainClassifier()
    val timeEnd = System.currentTimeMillis()

    // Evaluate model on test instances and compute test error
    val testingDataSize = rfRunner.getDataLoader.getTestingData.count()
    println("Test Data Size: " + testingDataSize)

    val labelAndPreds = rfRunner.getDataLoader.getTestingData.map { point =>
      val prediction = model.predict(point.features, property.getMaxDepth)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testingDataSize

    println("Test Error featurePerNode: " + model.getFeaturePerNode + " bin: " + model.getBinNumber + " (depth " + model.getDepth + ")(trees "+model.getNumTrees+") = " + (1 - testErr))
    CCUtilIO.logACCURACY(property, model, model.getDepth, (1 - testErr), (timeEnd - timeStart))

    //    rfRunner.printTree()
    rfRunner.sparkStop()
    println("Time: " + (timeEnd - timeStart))
  }
}
