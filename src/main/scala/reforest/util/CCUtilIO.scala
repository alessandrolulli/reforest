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

package reforest.util

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Calendar

import reforest.rf.RFModel
import reforest.rf.parameter.RFParameter

/**
  * An utility to perform IO
  */
object CCUtilIO extends Serializable {

  // property: RFProperty
  private val hourFormat = new SimpleDateFormat("hh:mm:ss")

  /**
    * It prints a list of values to a specified file
    *
    * @param file the file to use for writing
    * @param data the list of values to write on the specified file
    * @return
    */
  def printToFile(property: RFParameter, file: String, data: String*): Int = {
    if (property.logStats) {
      val printFile = new FileWriter(file, true)
      printFile.write(data.mkString(",") +
        ",strategyFeature," + property.strategyFeature.getDescription +
        ",strategySplit," + property.strategySplit.getDescription +
        ",fcsCycleActivation," + property.slcCycleActivation +
        ",numTrees," + property.getMaxNumTrees.toString +
        ",maxDepth," + property.getMaxDepth.toString +
        ",binNumber," + property.getMaxBinNumber.toString +
        ",sparkCoresMax," + property.sparkCoresMax.toString +
        ",sparkExecutorInstances," + property.sparkExecutorInstances.toString +
        ",numRotation," + property.numRotation.toString +
        ",uuid," + property.UUID + "\n")
      printFile.close()
    }
    0
  }

  /**
    * It logs the time at which a specific event happens
    *
    * @param algo  the algorithm name
    * @param event the event
    */
  def logTIME(property: RFParameter, algo: String, event: String) = {
    if (property.logStats) {
      val printFile = new FileWriter("time-event.txt", true)
      val today = Calendar.getInstance().getTime()
      val currentTime = hourFormat.format(today)
      printFile.write(currentTime + "," + algo + "," + event + "," + property.UUID + "\n")
      printFile.close()
    }
  }

  def roundAt(p: Int)(n: Double): Double = {
    val s = math pow(10, p)
    (math round n * s) / s
  }

  def logTIME(property: RFParameter,
              timeALL: Long,
              timeChecking: Long) = {
    val printFile = new FileWriter("stats-time.txt", true)

    val token = Array(
      property.appName,
      property.dataset,
      "TIME-ALL", timeALL,
      "TIME-COMPUTATION", timeALL - timeChecking,
      "TIME-CHECKING", timeChecking,
      if (property.slcActive) "SLC" else "NOT-SLC",
      if (property.modelSelection) "MS" else "NOT-MS",
      if (property.rotation) "ROTATION-" + property.numRotation else "NOT-ROTATION",
      property.UUID
    )

    printFile.write(token.mkString(",") + "\n")
    printFile.flush()
    printFile.close()
  }


  def logACCURACY[T, U](property: RFParameter,
                        model: RFModel[T, U],
                        depth: Int,
                        accuracy: Double,
                        time: Long,
                        fileName: String = "stats.txt") = {
    if (property.logStats) {
      val printFile = new FileWriter(fileName, true)

      val token = Array(
        property.appName,
        property.dataset,
        "ACCURACY", roundAt(5)(accuracy),
        "TIME", time,
        "DEPTH", depth,
        "BINNUMBER", model.getBinNumber,
        "NUMTREES", model.getNumTrees,
        "FEATURE", model.getFeaturePerNode,
        if (property.slcActive) "SLC" else "NOT-SLC",
        if (property.modelSelection) "MS" else "NOT-MS",
        if (property.rotation) "ROTATION-" + property.numRotation else "NOT-ROTATION",
        property.UUID
      )

      printFile.write(token.mkString(",") + "\n")
      printFile.flush()
      printFile.close()
    }
  }

  def logACCURACY[T, U](property: RFParameter,
                        accuracy: Double,
                        time: Long) = {
    if (property.logStats) {
      val printFile = new FileWriter("stats.txt", true)

      val token = Array(
        property.appName,
        property.dataset,
        roundAt(5)(accuracy),
        time,
        property.slcActive,
        property.UUID
      )

      printFile.write(token.mkString(",") + "\n")
      printFile.flush()
      printFile.close()
    }
  }

  /**
    * It logs some values to a standard file
    *
    * @param data the values to write
    */
  def log(data: String*) = {
    //    val printFile = new FileWriter("log.txt", true)
    //    printFile.write(data.mkString("\n")+"\n")
    //    printFile.close
  }
}