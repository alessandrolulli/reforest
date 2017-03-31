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

import java.io.{FileInputStream, InputStream}
import java.util.Properties


class CCProperties(algorithmName: String, configurationFile: String) extends Serializable {
  val property = new Properties

  def load(): CCProperties = {
    var input: InputStream = null

    input = new FileInputStream(configurationFile);

    property.load(input);

    this
  }

  def get(data: String, default: String) = {
    property.getProperty(data, default)
  }

  def getBoolean(data: String, default: Boolean) = {
    get(data, default.toString).toBoolean
  }

  def getInt(data: String, default: Int) = {
    get(data, default.toString).toInt
  }

  def getLong(data: String, default: Long) = {
    get(data, default.toString).toLong
  }

  def getDouble(data: String, default: Double) = {
    get(data, default.toString).toDouble
  }

  def getImmutable: CCPropertiesImmutable = {
    val dataset = get("dataset", "")
    val jarPath = get("jarPath", "")
    val sparkMaster = get("sparkMaster", "local[2]")
    val sparkExecutorMemory = get("sparkExecutorMemory", "14g")
    val sparkPartition = get("sparkPartition", "32").toInt
    val sparkBlockManagerSlaveTimeoutMs = get("sparkBlockManagerSlaveTimeoutMs", "500000")
    val sparkCoresMax = get("sparkCoresMax", "-1").toInt
    val sparkAkkaFrameSize = get("sparkAkkaFrameSize", "100").toString
    val sparkShuffleManager = get("sparkShuffleManager", "SORT").toString
    val sparkCompressionCodec = get("sparkCompressionCodec", "lz4").toString
    val sparkShuffleConsolidateFiles = get("sparkShuffleConsolidateFiles", "false").toString
    val sparkDriverMaxResultSize = get("sparkDriverMaxResultSize", "1g").toString
    var separator = get("edgelistSeparator", "space")
    if (separator.equals("space")) separator = " "
    val outputFile = get("outputFile", "")
    val sparkExecutorInstances = get("sparkExecutorInstances", "-1").toInt
    val instrumented = get("instrumented", "false").toBoolean

    new CCPropertiesImmutable(this, algorithmName,
      dataset,
      outputFile,
      jarPath,
      sparkMaster,
      sparkPartition,
      sparkExecutorMemory,
      sparkBlockManagerSlaveTimeoutMs,
      sparkCoresMax,
      sparkShuffleManager,
      sparkCompressionCodec,
      sparkShuffleConsolidateFiles,
      sparkAkkaFrameSize,
      sparkDriverMaxResultSize,
      sparkExecutorInstances,
      separator,
      instrumented)
  }
}