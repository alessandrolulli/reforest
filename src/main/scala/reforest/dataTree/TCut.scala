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

/**
  * It represents a cut in a node of a tree
  * @tparam T raw data type
  * @tparam U working data type
  */
trait TCut[T, U] extends Serializable {
  /**
    * It tells if the data passed as argument must navigate in the tree using the left child
    * @param data the data to navigate the tree
    * @param typeInfo the type information of the raw data
    * @return true if the navigation must proceed on the left child
    */
  def shouldGoLeft(data: RawDataLabeled[T, U], typeInfo: TypeInfo[T]): Boolean

  /**
    * It tells if the data passed as argument must navigate in the tree using the left child
    * @param data the data to navigate the tree
    * @param typeInfo the type information of the raw data
    * @return true if the navigation must proceed on the left child
    */
  def shouldGoLeft(data: RawData[T, U], typeInfo: TypeInfo[T]): Boolean

  /**
    * It tells if the working data passed as argument must navigate in the tree using the left child
    * @param data the working data to navigate the tree
    * @param typeInfo the type information of the working data
    * @return true if the navigation must proceed on the left child
    */
  def shouldGoLeftBin(data: WorkingData[U], typeInfo: TypeInfo[U]): Boolean

  /**
    * It returns a compressed representation of this cut
    * @return a compressed representation of this cut
    */
  def compress() : TCut[T, U]
}

/**
  * A minimal implementation of a cut
  * @param idFeature the feature index chosen for the cut
  * @param value the cut value to split the data
  * @param bin the discretized value to split the data
  * @tparam T raw data type
  * @tparam U working data type
  */
class Cut[T, U](val idFeature: Int,
                val value: T,
                val bin: U) extends TCut[T,U] {

  override def shouldGoLeft(data: RawDataLabeled[T, U], typeInfo: TypeInfo[T]): Boolean = {
    shouldGoLeft(data.features, typeInfo)
  }

  override def shouldGoLeft(data: RawData[T, U], typeInfo: TypeInfo[T]): Boolean = {
    typeInfo.isMinOrEqual(data(idFeature), value)
  }

  override def shouldGoLeftBin(data: WorkingData[U], typeInfo: TypeInfo[U]): Boolean = {
    typeInfo.isMinOrEqual(data(idFeature), bin)
  }

  override def toString = "("+idFeature+","+value+","+bin+")"

  override def compress() : TCut[T, U] = this
}

/**
  * A detailed cut. It contains all the information to grow a tree
  * @param idFeature the feature index chosen for the cut
  * @param value the cut value to split the data
  * @param bin the discretized value to split the data
  * @param stats the gain of this cut
  * @param label the predicted label of the node storing this cut
  * @param notValid the number of not valid element
  * @param left the number of element that will be using the left child
  * @param right the number of element that will be using the right child
  * @param labelNotValid the class label of the not valid element
  * @param labelLeft the class label of the left child
  * @param labelRight the class label of the right child
  * @param labelNotValidOk the number of element having the predicted label for not valid element
  * @param labelLeftOk the number of element having the predicted label for the one using the left child
  * @param labelRightOk the number of element having the predicted label for the one using the right child
  * @tparam T raw data type
  * @tparam U working data type
  */
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

  /**
    * To convert this cut in a cut amenable for not valid element
    * @param typeInfo raw data type
    * @param typeInfoWorking working data type
    * @return
    */
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

/**
  * A cut for not valid elements
  * @param idFeature the feature index chosen for the cut
  * @param value the cut value to split the data
  * @param stats the gain of this cut
  * @param not
  * @param label the predicted label of the node storing this cut
  * @param notValid the number of not valid element
  * @param left the number of element that will be using the left child
  * @param right the number of element that will be using the right child
  * @param labelNotValid the class label of the not valid element
  * @param labelLeft the class label of the left child
  * @param labelRight the class label of the right child
  * @param labelNotValidOk the number of element having the predicted label for not valid element
  * @param labelLeftOk the number of element having the predicted label for the one using the left child
  * @param labelRightOk the number of element having the predicted label for the one using the right child
  * @tparam T raw data type
  * @tparam U working data type
  */
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

/**
  * A compressed representation for the CutNotValid
  * @param idFeature the feature index chosen for the cut
  * @param value the cut value to split the data
  * @param not
  * @tparam T raw data type
  * @tparam U working data type
  */
class CutNotValidCompressed[T, U](idFeature: Int, value: T, not : U) extends Cut[T, U](idFeature, value, not) {
  override def shouldGoLeft(data: RawDataLabeled[T, U], typeInfo: TypeInfo[T]): Boolean = {
    if (data.features(idFeature) == 0) return true else false
  }
}

/**
  * A cut for categorical feature
  * @param idFeature the feature index chosen for the cut
  * @param value the cut value to split the data
  * @param stats the gain of this cut
  * @param not
  * @param label the predicted label of the node storing this cut
  * @param notValid the number of not valid element
  * @param left the number of element that will be using the left child
  * @param right the number of element that will be using the right child
  * @param labelNotValid the class label of the not valid element
  * @param labelLeft the class label of the left child
  * @param labelRight the class label of the right child
  * @param labelNotValidOk the number of element having the predicted label for not valid element
  * @param labelLeftOk the number of element having the predicted label for the one using the left child
  * @param labelRightOk the number of element having the predicted label for the one using the right child
  * @tparam T raw data type
  * @tparam U working data type
  */
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

/**
  * A compressed representation for a categorical cut
  * @param idFeature the feature index chosen for the cut
  * @param value the cut value to split the data
  * @param not
  * @tparam T raw data type
  * @tparam U working data type
  */
class CutCategoricalCompressed[T, U](idFeature: Int, value: T, not : U) extends Cut[T, U](idFeature, value, not) {
  override def shouldGoLeft(data: RawDataLabeled[T, U], typeInfo: TypeInfo[T]): Boolean = {
    if (data.features(idFeature) == value) return true else false
  }
}
