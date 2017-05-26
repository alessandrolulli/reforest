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

import org.apache.commons.io.FilenameUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import reforest.TypeInfo
import reforest.data.load.{ARFFUtil, DataLoad, LibSVMUtil}
import reforest.rf.{RFCategoryInfo, RFProperty}

import scala.reflect.ClassTag

/**
  * An utility class
  */
object CCUtil extends Serializable {
  /**
    * It returns a Spark Context using the configurations loaded from file
    * @return the Spark Context
    */
  def getSparkContext(property : RFProperty): SparkContext = {
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
      .set("spark.driver.maxResultSize", 0.toString)
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

  /**
    * It returns the utility to load the dataset from file. There are available multiple data loader for different
    * file formats (LibSVM, ARFF)
    * @param typeInfo the type information for the raw data
    * @param instrumented the instrumentations for the GC
    * @param categoryInfo the informations for categorical features
    * @tparam T raw data type
    * @tparam U working data type
    * @return the data loader specialized for the format of the dataset
    */
  def getDataLoader[T:ClassTag, U:ClassTag](property : RFProperty,
                                             typeInfo: Broadcast[TypeInfo[T]],
                                   instrumented: Broadcast[GCInstrumented],
                                   categoryInfo: Broadcast[RFCategoryInfo]): DataLoad[T, U] = {
    val extension = FilenameUtils.getExtension(property.dataset).toUpperCase()

    property.fileType match {
      case "LIBSVM" => new LibSVMUtil(typeInfo, instrumented, categoryInfo)
      case "SVM" => new LibSVMUtil(typeInfo, instrumented, categoryInfo)
      case "ARFF" => new ARFFUtil(typeInfo, instrumented, categoryInfo)
      case _ => new LibSVMUtil(typeInfo, instrumented, categoryInfo)
    }
  }
}
