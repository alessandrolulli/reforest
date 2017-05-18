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

package reforest.dataTree

import reforest.TypeInfo
import reforest.data.{RawData, RawDataLabeled, WorkingData}

trait TCut[T, U] extends Serializable {
  def shouldGoLeft(data: RawDataLabeled[T, U], typeInfo: TypeInfo[T]): Boolean
  def shouldGoLeft(data: RawData[T, U], typeInfo: TypeInfo[T]): Boolean
  def shouldGoLeftBin(data: WorkingData[U], typeInfo: TypeInfo[U]): Boolean
  def compress() : TCut[T, U]
}

class Cut[T, U](val idFeature: Int,
                val value: T,
                val bin: U) extends TCut[T,U] {

  def shouldGoLeft(data: RawDataLabeled[T, U], typeInfo: TypeInfo[T]): Boolean = {
    shouldGoLeft(data.features, typeInfo)
  }

  def shouldGoLeft(data: RawData[T, U], typeInfo: TypeInfo[T]): Boolean = {
    typeInfo.leq(data(idFeature), value)
  }

  def shouldGoLeftBin(data: WorkingData[U], typeInfo: TypeInfo[U]): Boolean = {
    typeInfo.isMinOrEqual(data(idFeature), bin)
  }

  override def toString = "("+idFeature+","+value+","+bin+")"

  def compress() : TCut[T, U] = this
}

class CutDetailed[T, U](idFeature: Int,
                        value: T,
                        bin: U,
                        val stats: Double = Double.MinValue,
                        val label: Option[Int] = Option.empty,
                        val notValid: Int = 0,
                        val left: Int = 0,
                        val right: Int = 0,
                        val labelNotValid: Option[Int] = Option.empty,
                        val labelLeft: Option[Int] = Option.empty,
                        val labelRight: Option[Int] = Option.empty,
                        val labelNotValidOk: Int = 0,
                        val labelLeftOk: Int = 0,
                        val labelRightOk: Int = 0) extends Cut[T,U](idFeature, value, bin) {
  override def compress() = {
    new Cut[T, U](idFeature, value, bin)
  }

  def getNotValid(typeInfo: TypeInfo[T], typeInfoWorking: TypeInfo[U]) = {
    new CutNotValid[T, U](idFeature, typeInfo.NaN, stats, typeInfoWorking.NaN, label, notValid, left, right, labelNotValid, labelLeft, labelRight, labelNotValidOk, labelLeftOk, labelRightOk)
  }

  override def toString: String = {
    val toPrint = Array(idFeature.toString, value.toString, bin.toString, stats.toString, label.toString, notValid.toString, left.toString, right.toString, labelNotValid.toString, labelLeft.toString, labelRight.toString, labelNotValidOk.toString, labelLeftOk.toString, labelRightOk.toString)
    "(" + toPrint.mkString(",") + ")"
  }
}

object CutDetailed {
  val empty = new CutDetailed(-1, -1, -1, Double.MinValue)
}

class CutNotValid[T, U](override val idFeature: Int,
                        override val value: T,
                        override val stats: Double,
                        not: U,
                        override val label: Option[Int] = Option.empty,
                        override val notValid: Int = 0,
                        override val left: Int = 0,
                        override val right: Int = 0,
                        override val labelNotValid: Option[Int] = Option.empty,
                        override val labelLeft: Option[Int] = Option.empty,
                        override val labelRight: Option[Int] = Option.empty,
                        override val labelNotValidOk: Int = 0,
                        override val labelLeftOk: Int = 0,
                        override val labelRightOk: Int = 0) extends CutDetailed[T, U](idFeature, value, not, stats) {
  override def shouldGoLeft(data: RawDataLabeled[T, U], typeInfo: TypeInfo[T]): Boolean = {
    if (data.features(idFeature) == 0) return true else false
  }

  override def compress() = {
    new CutNotValidCompressed[T, U](idFeature, value, bin)
  }
}

class CutNotValidCompressed[T, U](idFeature: Int, value: T, not : U) extends Cut[T, U](idFeature, value, not) {
  override def shouldGoLeft(data: RawDataLabeled[T, U], typeInfo: TypeInfo[T]): Boolean = {
    if (data.features(idFeature) == 0) return true else false
  }
}

class CutCategorical[T, U](override val idFeature: Int,
                        override val value: T,
                        override val stats: Double,
                        not: U,
                        override val label: Option[Int] = Option.empty,
                        override val notValid: Int = 0,
                        override val left: Int = 0,
                        override val right: Int = 0,
                        override val labelNotValid: Option[Int] = Option.empty,
                        override val labelLeft: Option[Int] = Option.empty,
                        override val labelRight: Option[Int] = Option.empty,
                        override val labelNotValidOk: Int = 0,
                        override val labelLeftOk: Int = 0,
                        override val labelRightOk: Int = 0) extends CutDetailed[T, U](idFeature, value, not, stats) {
  override def shouldGoLeft(data: RawDataLabeled[T, U], typeInfo: TypeInfo[T]): Boolean = {
    if (data.features(idFeature) == value) return true else false
  }

  override def compress() = {
    new CutCategoricalCompressed[T, U](idFeature, value, bin)
  }

  override def toString: String = "CATEGORY: "+super.toString
}

class CutCategoricalCompressed[T, U](idFeature: Int, value: T, not : U) extends Cut[T, U](idFeature, value, not) {
  override def shouldGoLeft(data: RawDataLabeled[T, U], typeInfo: TypeInfo[T]): Boolean = {
    if (data.features(idFeature) == value) return true else false
  }
}
