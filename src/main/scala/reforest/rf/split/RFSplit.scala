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

package reforest.rf.split

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import reforest.TypeInfo
import reforest.data.{RawDataDense, RawDataLabeled, RawDataSparse}
import reforest.rf.RFCategoryInfo
import reforest.util.GCInstrumented

import scala.collection.{Map, mutable}
import scala.reflect.ClassTag

/**
  * Forked from Apache Spark MLlib
  */
class RFSplit[T: ClassTag, U: ClassTag](typeInfo: Broadcast[TypeInfo[T]],
                                        typeInfoWorking: Broadcast[TypeInfo[U]],
                                        instrumented: Broadcast[GCInstrumented],
                                        categoricalFeatureInfo : Broadcast[RFCategoryInfo]) extends Serializable {

  def findSplitsSimple(input: RDD[RawDataLabeled[T, U]],
                       binNumber: Int,
                       featureNumber: Int,
                       featurePerIteration: Int): RFSplitter[T, U] = {

    val (min, max) = input.map(t => (typeInfo.value.getMin(t.features), typeInfo.value.getMax(t.features)))
      .reduce((a, b) => (typeInfo.value.min(a._1, b._1), typeInfo.value.max(a._2, b._2)))

    new RFSplitterSimple[T, U](min, max, typeInfo.value, typeInfoWorking.value, binNumber, categoricalFeatureInfo.value)
  }

  def findSplitsBySorting(input: RDD[RawDataLabeled[T, U]],
                          binNumber: Int,
                          featureNumber: Int,
                          featurePerIteration: Int): Map[Int, Array[T]] = {

    val iterationNumber = Math.ceil(featureNumber.toDouble / featurePerIteration).toInt
    var iteration = 0

    var continuousSplits: scala.collection.Map[Int, Array[T]] = Map.empty
    while (iteration < iterationNumber) {
      continuousSplits = continuousSplits ++ input
        .flatMap(point => {
          instrumented.value.gc()
          point.features match {
            case v: RawDataSparse[T, U] => {
              for (i <- iteration * featurePerIteration to Math.min(((iteration + 1) * featurePerIteration - 1), featureNumber - 1)) yield {
                (i, v(i))
              }
            }
            case v: RawDataDense[T, U] => {
              for (i <- iteration * featurePerIteration to Math.min(((iteration + 1) * featurePerIteration - 1), featureNumber - 1)) yield {
                (i, v(i))
              }
            }
            case _ => throw new ClassCastException
          }
        })
        .groupByKey()
        .map { case (idx, samples) =>
          val thresholds = findSplitsForContinuousFeature(samples, binNumber)
          instrumented.value.gc()
          (idx, thresholds)
        }.collectAsMap()
      iteration += 1
    }

    continuousSplits
  }

  def findSplitsForContinuousFeature(featureSamples: Iterable[T], binNumber: Int): Array[T] = {
    val splits: Array[T] = if (featureSamples.isEmpty) {
      Array.empty
    } else {
      val numSplits = binNumber

      // get count for each distinct value
      val (valueCountMap, numSamples) = featureSamples.filter(t => typeInfo.value.isValidForBIN(t)).foldLeft((Map.empty[T, Int], 0)) {
        case ((m, cnt), x) =>
          (m + ((x, m.getOrElse(x, 0) + 1)), cnt + 1)
      }
      // sort distinct values
      val valueCounts = valueCountMap.toSeq.sortBy(_._1)(typeInfo.value.getOrdering).toArray

      // if possible splits is not enough or just enough, just return all possible splits
      val possibleSplits = valueCounts.length - 1
      if (possibleSplits <= numSplits) {
        if (possibleSplits <= 0) Array.fill(1)(typeInfo.value.NaN)
        else
          valueCounts.map(_._1).init
      } else {
        // stride between splits
        val stride: Double = numSamples.toDouble / (numSplits + 1)

        // iterate `valueCount` to find splits
        val splitsBuilder = mutable.ArrayBuilder.make[T]
        var index = 1
        // currentCount: sum of counts of values that have been visited
        var currentCount = valueCounts(0)._2
        // targetCount: target value for `currentCount`.
        // If `currentCount` is closest value to `targetCount`,
        // then current value is a split threshold.
        // After finding a split threshold, `targetCount` is added by stride.
        var targetCount = stride
        while (index < valueCounts.length) {
          val previousCount = currentCount
          currentCount += valueCounts(index)._2
          val previousGap = math.abs(previousCount - targetCount)
          val currentGap = math.abs(currentCount - targetCount)
          // If adding count of current value to currentCount
          // makes the gap between currentCount and targetCount smaller,
          // previous value is a split threshold.
          if (previousGap < currentGap) {
            splitsBuilder += valueCounts(index - 1)._1
            targetCount += stride
          }
          index += 1
        }

        splitsBuilder.result()
      }
    }
    splits
  }
}
