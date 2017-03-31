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

import org.apache.spark.{SparkConf, SparkContext}

class CCUtil(val property: CCPropertiesImmutable) extends Serializable {
  val io = new CCUtilIO(property)
  var vertexNumber = 0L

  def getSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setMaster(property.sparkMaster)
      .setAppName(property.appName)
      .set("spark.executor.memory", property.sparkExecutorMemory)
      .set("spark.storage.blockManagerSlaveTimeoutMs", property.sparkBlockManagerSlaveTimeoutMs)
      .set("spark.shuffle.manager", property.sparkShuffleManager)
      .set("spark.shuffle.consolidateFiles", property.sparkShuffleConsolidateFiles)
      .set("spark.io.compression.codec", property.sparkCompressionCodec)
      .set("spark.akka.frameSize", property.sparkAkkaFrameSize)
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.maxResultSize", property.sparkDriverMaxResultSize)
      .set("spark.core.connection.ack.wait.timeout", 600.toString)
    //				.set("spark.task.cpus", "8")
    //	.setJars(Array(property.jarPath)
    //)

    if (property.sparkCoresMax > 0) {
      conf.set("spark.cores.max", property.sparkCoresMax.toString)
      val executorCore = property.sparkCoresMax / property.sparkExecutorInstances
      conf.set("spark.executor.cores", executorCore.toString)
    }
    if (property.sparkExecutorInstances > 0)
      conf.set("spark.executor.instances", property.sparkExecutorInstances.toString)

    //conf.registerKryoClasses(Array(classOf[scala.collection.mutable.HashMap[_, _]]))
    val spark = new SparkContext(conf)

    spark
  }
}
