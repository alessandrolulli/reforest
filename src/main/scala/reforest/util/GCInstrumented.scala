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

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.spark.{SparkContext, TaskContext}

trait GCInstrumented extends Serializable {
  def start()

  def stop()

  def gc()

  def gc(f: => Any)

  def gcALL()

  def valid: Boolean
}

class GCInstrumentedEmpty extends GCInstrumented {
  def gc() = {}

  def gc(f: => Any) = f

  def gcALL() = {}

  val valid = false

  override def start() = {}

  override def stop() = {}
}

class GCInstrumentedFull(@transient val sc: SparkContext) extends GCInstrumented {

  val dataForGC = sc.parallelize(Seq.fill(10000)(0)).cache()

  override def start() = {
    dataForGC.foreachPartition(t => GCRunner.start())
  }

  override def stop() = {
    dataForGC.foreachPartition(t => GCRunner.stop())
  }

  def gc() = {
    System.gc()
  }

  def gc(f: => Any) = {
    gc
    f
    gc
  }

  def gcALL() = {
    dataForGC.foreachPartition(t => System.gc())
  }

  val valid = true
}

object GCRunner {
  val ex = new ScheduledThreadPoolExecutor(1)

  var partitionIndex: Option[Int] = Option.empty
  var f: Option[ScheduledFuture[_]] = Option.empty

  def start() = {
    this.synchronized({
      val taskId = TaskContext.getPartitionId()
      if (partitionIndex.isEmpty) {
        partitionIndex = Some(taskId)
        val task = new Runnable {
          def run() = System.gc()
        }
        f = Some(ex.scheduleAtFixedRate(task, 1, 1, TimeUnit.SECONDS))
        f.get.cancel(false)
      }
    })
  }

  def stop() = {
    if (f.isDefined) {
      f.get.cancel(true)
      f = Option.empty
      partitionIndex = Option.empty
    }
  }
}
