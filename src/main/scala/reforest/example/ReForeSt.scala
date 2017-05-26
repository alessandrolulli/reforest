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

import reforest.rf.{RFProperty, RFPropertyFile, RFRunner}
import reforest.util.CCProperties

/**
  * An example to use the ReForeSt library to perform Random Forest
  */
object ReForeSt {

  def main(args: Array[String]): Unit = {

    // Create the ReForeSt configuration.
    val property = new RFProperty()
    property.dataset = "data/sample-infimnist.libsvm"
    property.featureNumber = 794

    property.numTrees = 100
    property.maxDepth = 5
    property.numClasses = 10

    // Create the Random Forest classifier.
    val rfRunner = RFRunner.apply(property)

    // Load and parse the data file and return the training data.
    val trainingData = rfRunner.loadData(0.7)

    // Train a Random Forest model.
    val model = rfRunner.trainClassifier(trainingData)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = rfRunner.getTestData().map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / rfRunner.getTestData().count()
    rfRunner.printTree()
    rfRunner.sparkStop()

    println("Test Error = " + testErr)
  }
}
