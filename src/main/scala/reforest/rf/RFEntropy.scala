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

package reforest.rf

import org.apache.spark.broadcast.Broadcast
import reforest.TypeInfo
import reforest.dataTree.{Cut, CutCategorical}
import reforest.rf.split.RFSplitter

class RFEntropy[T, U](typeInfo: Broadcast[TypeInfo[T]],
                      typeInfoWorking: Broadcast[TypeInfo[U]]) extends Serializable {

  def entropyFromPreComputedArray(classAccumulator: Array[Int], numElement: Int) = {
    var toReturn = 0d

    var c = 0
    while (c < classAccumulator.length) {
      val tmp = classAccumulator(c).toDouble / numElement
      if (tmp > 0) {
        toReturn += (-tmp * Math.log(tmp))
      }
      c += 1
    }

    toReturn
  }

  def entropy(valueArray: Array[Array[Int]], numElement: Int, numClasses: Int, start: Int, end: Int): Double = {
    val classAccumulator = new Array[Int](numClasses)

    var i = start
    while(i <= end) {
      var c = 0
      while (c < valueArray(i).length) {
        classAccumulator(c) += valueArray(i)(c)
        c += 1
      }
      i += 1
    }

    entropyFromPreComputedArray(classAccumulator, numElement)
  }

  def entropyCategoryExclude(valueArray: Array[Array[Int]], numElement: Int, numClasses: Int, categoryIndex: Int): Double = {
    val classAccumulator = new Array[Int](numClasses)

    var i = 1
    while(i <= valueArray.length - 1) {
      if (i != categoryIndex) {
        var c = 0
        while (c < valueArray(i).length) {
          classAccumulator(c) += valueArray(i)(c)
          c += 1
        }
      }
      i += 1
    }

    entropyFromPreComputedArray(classAccumulator, numElement)
  }

  def entropyCategory(valueArray: Array[Array[Int]], numElement: Int, numClasses: Int, categoryIndex: Int): Double = {
    val classAccumulator = new Array[Int](numClasses)

    var c = 0
    while (c < valueArray(categoryIndex).length) {
      classAccumulator(c) += valueArray(categoryIndex)(c)
      c += 1
    }

    entropyFromPreComputedArray(classAccumulator, numElement)
  }

  def entropy(valueArray: Array[Array[Int]], numClasses: Int): Double = {
    val numElement = sum(valueArray)
    entropy(valueArray, numElement, numClasses, 0, valueArray.length - 1)
  }

  def getLabel(valueArray: Array[Array[Int]], numClasses: Int): Option[Int] = {
    if (valueArray.isEmpty) {
      Option.empty
    } else {
      val classAccumulator = new Array[Int](numClasses)

      var count = 0
      while (count < valueArray.length) {
        var c = 0
        while (c < valueArray(count).length) {
          classAccumulator(c) += valueArray(count)(c)
          c += 1
        }
        count += 1
      }

      Some(classAccumulator.zipWithIndex.maxBy(_._1)._2)
    }
  }

  def getLabelOK(valueArray: Array[Array[Int]], label: Option[Int]): Int = {
    if (label.isDefined) {
      getLabelOK(valueArray, label.get)
    } else {
      0
    }
  }

  def getLabelOK(valueArray: Array[Array[Int]], label: Int): Int = {
    if (valueArray.isEmpty) {
      0
    } else {
      var toReturn = 0

      var count = 0
      while (count < valueArray.length) {
        toReturn += valueArray(count)(label)
        count += 1
      }

      toReturn
    }
  }

  def getLabel(valueArray: Array[Int]): Option[Int] = {
    Some(valueArray.zipWithIndex.maxBy(_._1)._2)
  }

  def sum(valueArray: Array[Array[Int]]): Int = {
    var toReturn = 0

    var count = 0
    while (count < valueArray.length) {
      var c = 0
      while (c < valueArray(count).length) {
        toReturn += valueArray(count)(c)
        c += 1
      }

      count += 1
    }

    toReturn
  }

  def sum(valueArray: Array[Int]): Int = {
    var toReturn = 0

    var count = 0
    while (count < valueArray.length) {
      toReturn += valueArray(count)
      count += 1
    }

    toReturn
  }

  def getBestSplit(data: Array[Array[Int]],
                   featureId: Int,
                   splitter: RFSplitter[T, U],
                   depth: Int,
                   maxDepth: Int,
                   numClasses: Int): Cut[T, U] = {
    val elNumber = sum(data)
    val elNumberValid = elNumber - sum(data(0))
    val elNumberNOTValid = elNumber - elNumberValid
    var gBest = Double.MinValue
    var cut = Int.MinValue
    val eTot = entropy(data, elNumber, numClasses, 0, data.length - 1)
    var elSum = 0
    if (elNumberValid > 0) {
      val until = Math.min(data.length - 1, typeInfoWorking.value.toInt(splitter.getBinNumber(featureId)))
      var i = 1
      while (i <= until) {
        val sumData = sum(data(i))
        if (sumData > 0) {
          elSum = elSum + sumData
          val g = eTot - ((elSum * entropy(data, elSum, numClasses, 1, i)) / elNumber) - (((elNumberValid - elSum) * entropy(data, elNumberValid - elSum, numClasses, i + 1, data.length - 1)) / elNumber)
          if (g > gBest) {
            gBest = g
            cut = i
          }
        }
        i += 1
      }
      val left = data.slice(1, cut + 1)
      val leftTOT = sum(left)
      val right = data.slice(cut + 1, data.length)
      val rightTOT = sum(right)
      val calculateLabel = depth >= maxDepth || leftTOT <= 1 || rightTOT <= 1 || elNumberNOTValid > 0
      val leftLabel = if (calculateLabel) getLabel(left, numClasses) else Option.empty
      val rightLabel = if (calculateLabel) getLabel(right, numClasses) else Option.empty
      val notValidLabel = if (elNumberNOTValid > 0) getLabel(data(0)) else Option.empty

      var eEnd = gBest
      if ((elNumber - elNumberValid) > 0) {
        val eNotValid = (((elNumber - elNumberValid) * entropy(Array(data(0)), numClasses)) / elNumber)
        eEnd = eEnd - eNotValid
      }

      val leftOK = getLabelOK(left, leftLabel)
      val rightOK = getLabelOK(right, rightLabel)
      val notvalidOK = if (notValidLabel.isDefined) data(0)(notValidLabel.get) else 0

      new Cut[T, U](featureId,
        splitter.getRealCut(featureId, typeInfoWorking.value.fromInt(cut)),
        typeInfoWorking.value.fromInt(cut),
        eEnd,
        if (calculateLabel) getLabel(data, numClasses) else Option.empty,
        (elNumber - elNumberValid),
        leftTOT,
        rightTOT,
        notValidLabel,
        leftLabel,
        rightLabel,
        notvalidOK,
        leftOK,
        rightOK)
    } else {
      new Cut(featureId, typeInfo.value.NaN, typeInfoWorking.value.NaN)
    }
  }

  def getBestSplitCategorical(data: Array[Array[Int]],
                              featureId: Int,
                              splitter: RFSplitter[T, U],
                              depth: Int,
                              maxDepth: Int,
                              numClasses: Int): Cut[T, U] = {
    val elNumber = sum(data)
    val elNumberValid = elNumber - sum(data(0))
    val elNumberNOTValid = elNumber - elNumberValid
    var gBest = Double.MinValue
    var cut = Int.MinValue
    val eTot = entropy(data, elNumber, numClasses, 0, data.length - 1)
    if (elNumberValid > 0) {
      val until = Math.min(data.length - 1, typeInfoWorking.value.toInt(splitter.getBinNumber(featureId)))
      var i = 1
      while (i <= until) {
        val elNumberCategory = sum(data(i))
        if (elNumberCategory > 0) {
          val elNumberNotCategory = elNumberValid - elNumberCategory
          val g = eTot - ((elNumberCategory * entropyCategory(data, elNumberCategory, numClasses, i)) / elNumber) - ((elNumberNotCategory * entropyCategoryExclude(data, elNumberNotCategory, numClasses, i)) / elNumber)
          if (g > gBest) {
            gBest = g
            cut = i
          }
        }
        i += 1
      }
      val left = data(cut)
      val leftTOT = sum(left)
      val right = data.take(cut) ++ data.drop(cut + 1)
      val rightTOT = sum(right)
      val calculateLabel = depth >= maxDepth || leftTOT <= 1 || rightTOT <= 1 || elNumberNOTValid > 0
      val leftLabel = if (calculateLabel) getLabel(left) else Option.empty
      val rightLabel = if (calculateLabel) getLabel(right, numClasses) else Option.empty
      val notValidLabel = if (elNumberNOTValid > 0) getLabel(data(0)) else Option.empty

      var eEnd = gBest
      if ((elNumber - elNumberValid) > 0) {
        val eNotValid = (((elNumber - elNumberValid) * entropy(Array(data(0)), numClasses)) / elNumber)
        eEnd = eEnd - eNotValid
      }

      val leftOK = if (leftLabel.isDefined) left(leftLabel.get) else 0
      val rightOK = getLabelOK(right, rightLabel)
      val notvalidOK = if (notValidLabel.isDefined) data(0)(notValidLabel.get) else 0

      new CutCategorical[T, U](featureId,
        typeInfo.value.fromInt(cut),
        eEnd,
        typeInfoWorking.value.fromInt(cut),
        if (calculateLabel) getLabel(data, numClasses) else Option.empty,
        (elNumber - elNumberValid),
        leftTOT,
        rightTOT,
        notValidLabel,
        leftLabel,
        rightLabel,
        notvalidOK,
        leftOK,
        rightOK)
    } else {
      new Cut(featureId, typeInfo.value.NaN, typeInfoWorking.value.NaN)
    }
  }
}
