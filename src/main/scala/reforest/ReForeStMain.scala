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

import reforest.rf.rotation.{RFRunnerRotation, RotationDataUtil}
import reforest.rf._
import reforest.util.{CCProperties, CCUtil, CCUtilIO}

object ReForeStMain {
  def main(args: Array[String]): Unit = {

    val property = new RFPropertyFile(new CCProperties("ReForeSt", args(0)).load().getImmutable)

    val sc = CCUtil.getSparkContext(property)
    sc.setLogLevel(property.loader.get("logLevel", "error"))

    val typeInfo = new TypeInfoByte

    val category = property.loader.get("category", "") match
    {
      case "" => new RFCategoryInfoEmpty
      case categoryValue : String => new RFCategoryInfoSpecialized(property.loader.get("categoryReMap", ""), categoryValue)
      case _ => new RFCategoryInfoEmpty
    }

    val rfRunner = property.strategy match {
      case "reforest" => {
        property.setAppName("reforest")
        RFRunner.apply(sc, property, property.strategyFeature, typeInfo, category)
      }
      case "rotation" => {
        property.setAppName("reforest-rotation")
        val dataUtil = new RotationDataUtil[Double, Byte](sc, property, sc.broadcast(new TypeInfoDouble), property.property.sparkCoresMax * 2)
        RFRunnerRotation.apply(sc, property, property.strategyFeature, dataUtil, typeInfo)
      }
    }

    val trainingData = rfRunner.loadData(0.7)

    val model = rfRunner.trainClassifier(trainingData)

    if (!property.skipAccuracy) {
      val labelAndPreds = rfRunner.getTestData().map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }

      val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / rfRunner.getTestData().count()
      println("Test Error = " + testErr)
      rfRunner.printTree()

      CCUtilIO.printToFile(property, "stats.txt", property.appName, property.property.dataset,
        "accuracy", testErr.toString,
        "time", rfRunner.getTrainingTime.toString
      )
    } else {
      CCUtilIO.printToFile(property, "stats.txt", property.appName, property.property.dataset,
        "accuracy", "NOT-COMPUTED",
        "time", rfRunner.getTrainingTime.toString
      )
    }
  }
}
