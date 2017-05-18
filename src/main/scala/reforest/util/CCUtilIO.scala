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

import reforest.rf.RFProperty

class CCUtilIO(property: RFProperty) extends Serializable {

  val hourFormat = new SimpleDateFormat("hh:mm:ss")

  def printToFile(file: String, data: String*): Int = {
    val printFile = new FileWriter(file, true)
    printFile.write(data.mkString(",")+
      ",strategy,"+property.strategy+
      ",strategyFeature,"+property.strategyFeature.getDescription+
      ",strategySplit,"+property.strategySplit.getDescription+
      ",fcsCycleActivation,"+property.fcsCycleActivation+
      ",numTrees,"+property.numTrees.toString+
      ",maxDepth,"+property.maxDepth.toString+
      ",binNumber,"+property.binNumber.toString+
      ",sparkCoresMax,"+property.property.sparkCoresMax.toString+
      ",sparkExecutorInstances,"+property.property.sparkExecutorInstances.toString+
      ",numRotation,"+property.numRotation.toString+
      ",uuid,"+property.uuid + "\n")
    printFile.close

    0
  }

  def logTIME(algo: String, event: String) = {
    val printFile = new FileWriter("time-event.txt", true)
    val today = Calendar.getInstance().getTime()
    val currentTime = hourFormat.format(today)
    printFile.write(currentTime+","+algo+","+event+","+property.uuid +"\n")
    printFile.close
  }

  def log(data: String*) = {
    //    val printFile = new FileWriter("log.txt", true)
    //    printFile.write(data.mkString("\n")+"\n")
    //    printFile.close
  }
}