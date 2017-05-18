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

package reforest

import reforest.data.{RawData, RawDataDense, RawDataSparse}

trait TypeInfo[T] extends Serializable {
  def zero : T

  def NaN: T

  def minValue : T

  def maxValue : T

  def isValidForBIN(value : T) : Boolean

  def isNOTvalidDefined : Boolean

  def fromString(s: String): T

  def fromInt(i: Int): T

  def fromDouble(i: Double): T

  def toInt(i: T): Int

  def toDouble(i: T): Double

  def mult[U](i: T, v : U, typeInfo: TypeInfo[U]) : T

  def leq(a: T, b: T): Boolean

  def getRealCut(cut: Int, split: Array[T]): T

  def getSimpleSplit[U](min : T, max : T, binNumber : Int, typeInfo: TypeInfo[U]) : (T => U)

  def getSimpleSplitInverted[U](min : T, max : T, binNumber : Int, typeInfo: TypeInfo[U]) : (U => T)

  def getIndex(array: Array[T], value: T): Int

  def getOrdering : Ordering[T]

  def getMin(data : RawData[T, _]) : T = {
    data match {
      case d : RawDataDense[T, _] => getMin(d.values)
      case d : RawDataSparse[T, _] => getMin(d.values)
      case _ => throw new ClassCastException
    }
  }

  def getMin(array : Array[T]) : T

  def getMax(data : RawData[T, _]) : T = {
    data match {
      case d : RawDataDense[T, _] => getMax(d.values)
      case d : RawDataSparse[T, _] => getMax(d.values)
      case _ => throw new ClassCastException
    }
  }

  def getMax(array : Array[T]) : T

  def min(a : T, b : T) : T

  def isMinOrEqual(a : T, b : T) : Boolean

  def max(a : T, b : T) : T
}

class TypeInfoDouble(override val isNOTvalidDefined : Boolean = false, override val NaN : Double = 0d) extends TypeInfo[Double] {
  override def zero: Double = 0
  override def minValue: Double = Double.MinValue
  override def maxValue: Double = Double.MaxValue
  override def fromString(s: String) = s.toDouble
  override def fromInt(i: Int): Double = i.toDouble
  override def fromDouble(i: Double): Double = if(i.isNaN) NaN else i
  override def toInt(i: Double): Int = i.toInt
  override def toDouble(i: Double): Double = i
  override def getOrdering: Ordering[Double] = scala.math.Ordering.Double
  override def getMin(array : Array[Double]) : Double = array.min
  override def getMax(array : Array[Double]) : Double = array.max
  override def min(a : Double, b : Double) : Double = if(a < b) a else b
  override def isMinOrEqual(a : Double, b : Double) : Boolean = a <= b
  override def max(a : Double, b : Double) : Double = if(a > b) a else b

  override def mult[U](v : Double, v2 : U, typeInfo : TypeInfo[U]) : Double = {
    typeInfo.toDouble(v2) * v
  }

  override def isValidForBIN(value : Double) : Boolean = !isNOTvalidDefined || (NaN match {
    case 0d => value != NaN
    case _ => !value.isNaN
  })

  override def leq(a: Double, b: Double): Boolean = a <= b

  override def getRealCut(cut: Int, info: Array[Double]) = {
    if (cut < 0)
      0d
    else if ((cut-1) >= info.size)
      Double.MaxValue
    else {
      info(cut - 1)
    }
  }

  override def getSimpleSplit[U](min : Double, max : Double, binNumber : Int, typeInfo: TypeInfo[U]) : (Double => U) = {
    val binSize = (max-min) / binNumber
    (value : Double) => {
      if(isNOTvalidDefined && value == NaN) typeInfo.zero else
      typeInfo.fromInt(Math.min(binNumber - 1, Math.max(1, Math.ceil((value - (min + binSize)) / binSize))).toInt)
    }
  }

  override def getSimpleSplitInverted[U](min : Double, max : Double, binNumber : Int, typeInfo: TypeInfo[U]) : (U => Double) = {
    val binSize = (max-min) / binNumber
    (value : U) => {
      min + (mult(binSize, value, typeInfo))
    }
  }

  override def getIndex(array: Array[Double], value: Double): Int = {
    java.util.Arrays.binarySearch(array, value)
  }
}

class TypeInfoFloat(override val isNOTvalidDefined : Boolean = false, override val NaN : Float = 0f) extends TypeInfo[Float] {
  override def zero: Float = 0
  override def minValue: Float = Float.MinValue
  override def maxValue: Float = Float.MaxValue
  override def fromString(s: String) = s.toFloat
  override def fromInt(i: Int): Float = i.toFloat
  override def fromDouble(i: Double): Float = if(i.isNaN) NaN else i.toFloat
  override def toInt(i: Float): Int = i.toInt
  override def toDouble(i: Float): Double = i
  override def getOrdering: Ordering[Float] = scala.math.Ordering.Float
  override def getMin(array : Array[Float]) : Float = array.min
  override def getMax(array : Array[Float]) : Float = array.max
  override def min(a : Float, b : Float) : Float = if(a < b) a else b
  override def isMinOrEqual(a : Float, b : Float) : Boolean = a <= b
  override def max(a : Float, b : Float) : Float = if(a > b) a else b

  override def mult[U](v : Float, v2 : U, typeInfo : TypeInfo[U]) : Float = {
    (typeInfo.toDouble(v2) * v).toFloat
  }

  override def isValidForBIN(value : Float) : Boolean = !isNOTvalidDefined || (NaN match {
    case 0f => value != NaN
    case _ => !value.isNaN
  })

  override def leq(a: Float, b: Float): Boolean = a <= b

  override def getRealCut(cut: Int, info: Array[Float]) = {
    if (cut < 0)
      0f
    else if ((cut-1) >= info.size)
      Float.MaxValue
    else {
      info(cut - 1)
    }
  }

  override def getSimpleSplit[U](min : Float, max : Float, binNumber : Int, typeInfo: TypeInfo[U]) : (Float => U) = {
    val binSize = (max-min) / binNumber
    (value : Float) => {
      if(isNOTvalidDefined && value == NaN) typeInfo.zero else
      typeInfo.fromInt(Math.min(binNumber - 1, Math.max(1, Math.ceil((value - (min + binSize)) / binSize))).toInt)
    }
  }

  override def getSimpleSplitInverted[U](min : Float, max : Float, binNumber : Int, typeInfo: TypeInfo[U]) : (U => Float) = {
    val binSize = (max-min) / binNumber
    (value : U) => {
      min + (mult(binSize, value, typeInfo))
    }
  }

  override def getIndex(array: Array[Float], value: Float): Int = {
    java.util.Arrays.binarySearch(array, value)
  }
}

//class TypeInfoFloat16(override val isNOTvalidDefined : Boolean = false, override val NaN : Float16 = new Float16(0)) extends TypeInfo[Float16] {
//  override def fromString(s: String) = Float16.fromFloat(s.toFloat)
//
//  override def fromInt(i: Int): Float16 = new Float16(i.toShort)
//
//  override def toInt(i: Float16): Int = i.raw.toInt
//
//  override def isValidForBIN(value : Float16) : Boolean = !isNOTvalidDefined || (NaN.raw match {
//    case 0 => value != 0
//    case _ => !value.isNaN
//  })
//
//  override def leq(a: Float16, b: Float16): Boolean = a <= b
//
//  override def getRealCut(cut: Int, info: Array[Float16]) = {
//    if (cut < 0)
//      new Float16(0)
//    else if (cut >= info.size)
//      Float16.MaxValue
//    else {
//      info(cut - 1)
//    }
//  }
//
//  override def getSimpleSplit(min : Float16, max : Float16, binNumber : Int) : (Float16 => Byte) = {
//    val binSize = (max-min) / new Float16(binNumber.toShort)
//    val fBin = new Float16((binNumber - 1).toShort)
//    (value : Float16) => {
//      val actualBin = (value - (min + binSize)) / binSize
//      val tmp = if(Float16.Zero > actualBin) Float16.Zero else actualBin
//      if(fBin < tmp) fBin.raw.toByte else tmp.raw.toByte
//    }
//  }
//
//  override def getSimpleSplitInverted(min : Float16, max : Float16, binNumber : Int) : (Byte => Float16) = {
//    val binSize = (max-min) / new Float16(binNumber.toShort)
//    (value : Byte) => {
//      min + (new Float16(value) * binSize)
//    }
//  }
//
//  override def getIndex(array: Array[Float16], value: Float16): Int = {
//    java.util.Arrays.binarySearch(array.map(t => t.raw), value.raw)
//  }
//
//  override def getOrdering: Ordering[Float16] = {
//    Ordering.by(e => e.raw)
//  }
//
//  override def getMin(array : Array[Float16]) : Float16 = array.min(getOrdering)
//
//  override def getMax(array : Array[Float16]) : Float16 = array.max(getOrdering)
//
//  override def min(a : Float16, b : Float16) : Float16 = if(a < b) a else b
//
//  override def max(a : Float16, b : Float16) : Float16 = if(a > b) a else b
//}

class TypeInfoByte(override val isNOTvalidDefined : Boolean =false, override val NaN : Byte = 0) extends TypeInfo[Byte] {
  override def zero: Byte = 0
  override def minValue: Byte = Byte.MinValue
  override def maxValue: Byte = Byte.MaxValue
  override def fromString(s: String) = s.toByte
  override def fromInt(i: Int): Byte = i.toByte
  override def fromDouble(i: Double): Byte = if(i.isNaN) NaN else i.toByte
  override def toInt(i: Byte): Int = i.toInt
  override def toDouble(i: Byte): Double = i
  override def getOrdering: Ordering[Byte] = scala.math.Ordering.Byte
  override def getMin(array : Array[Byte]) : Byte = array.min
  override def getMax(array : Array[Byte]) : Byte = array.max
  override def min(a : Byte, b : Byte) : Byte = if(a < b) a else b
  override def isMinOrEqual(a : Byte, b : Byte) : Boolean = a <= b
  override def max(a : Byte, b : Byte) : Byte = if(a > b) a else b

  override def mult[U](v : Byte, v2 : U, typeInfo : TypeInfo[U]) : Byte = {
    (typeInfo.toDouble(v2) * v).toByte
  }

  override def isValidForBIN(value : Byte) : Boolean = !isNOTvalidDefined || (NaN match {
    case 0 => value != NaN
    case _ => !value.isNaN
  })

  override def leq(a: Byte, b: Byte): Boolean = a <= b

  override def getRealCut(cut: Int, info: Array[Byte]) = {
    if (cut < 0)
      0.toByte
    else if ((cut-1) >= info.size)
      Byte.MaxValue
    else {
      info(cut - 1)
    }
  }

  override def getSimpleSplit[U](min : Byte, max : Byte, binNumber : Int, typeInfo: TypeInfo[U]) : (Byte => U) = {
    val binSize = (max-min) / binNumber
    (value : Byte) => {
      if(isNOTvalidDefined && value == NaN) typeInfo.zero else
      typeInfo.fromInt(Math.min(binNumber - 1, Math.max(1, Math.ceil((value - (min + binSize)) / binSize))).toInt)
    }
  }

  override def getSimpleSplitInverted[U](min : Byte, max : Byte, binNumber : Int, typeInfo: TypeInfo[U]) : (U => Byte) = {
    val binSize = ((max-min) / binNumber).toByte
    (value : U) => {
      (min + (mult(binSize, value, typeInfo))).toByte
    }
  }

  override def getIndex(array: Array[Byte], value: Byte): Int = {
    java.util.Arrays.binarySearch(array, value)
  }
}

class TypeInfoShort(override val isNOTvalidDefined : Boolean = false, override val NaN : Short = 0) extends TypeInfo[Short] {
  override def zero: Short = 0
  override def minValue: Short = Short.MinValue
  override def maxValue: Short = Short.MaxValue
  override def fromString(s: String) = s.toShort
  override def fromInt(i: Int): Short = i.toShort
  override def fromDouble(i: Double): Short = if(i.isNaN) NaN else i.toShort
  override def toInt(i: Short): Int = i.toInt
  override def toDouble(i: Short): Double = i
  override def leq(a: Short, b: Short): Boolean = a <= b
  override def getOrdering: Ordering[Short] = scala.math.Ordering.Short
  override def getMin(array : Array[Short]) : Short = array.min
  override def getMax(array : Array[Short]) : Short = array.max
  override def min(a : Short, b : Short) : Short = if(a < b) a else b
  override def isMinOrEqual(a : Short, b : Short) : Boolean = a <= b
  override def max(a : Short, b : Short) : Short = if(a > b) a else b

  override def mult[U](v : Short, v2 : U, typeInfo : TypeInfo[U]) : Short = {
    (typeInfo.toDouble(v2) * v).toShort
  }

  override def isValidForBIN(value : Short) : Boolean = !isNOTvalidDefined || (NaN match {
    case 0 => value != NaN
    case _ => !value.isNaN
  })

  override def getRealCut(cut: Int, info: Array[Short]) = {
    if (cut < 0)
      0.toShort
    else if ((cut-1) >= info.size)
      Short.MaxValue
    else {
      info(cut - 1)
    }
  }

  override def getSimpleSplit[U](min : Short, max : Short, binNumber : Int, typeInfo: TypeInfo[U]) : (Short => U) = {
    val binSize = (max-min) / binNumber
    (value : Short) => {
      if(isNOTvalidDefined && value == NaN) typeInfo.zero else
      typeInfo.fromInt(Math.min(binNumber - 1, Math.max(1, Math.ceil((value - (min + binSize)) / binSize))).toInt)
    }
  }

  override def getSimpleSplitInverted[U](min : Short, max : Short, binNumber : Int, typeInfo: TypeInfo[U]) : (U => Short) = {
    val binSize = ((max-min) / binNumber).toShort
    (value : U) => {
      (min + (mult(binSize, value, typeInfo))).toShort
    }
  }

  override def getIndex(array: Array[Short], value: Short): Int = {
    java.util.Arrays.binarySearch(array, value)
  }
}

class TypeInfoInt(override val isNOTvalidDefined : Boolean = false, override val NaN : Int = 0) extends TypeInfo[Int] {
  override def zero: Int = 0
  override def minValue: Int = Int.MinValue
  override def maxValue: Int = Int.MaxValue
  override def fromString(s: String) = s.toInt
  override def fromInt(i: Int): Int = i
  override def fromDouble(i: Double): Int = if(i.isNaN) NaN else i.toInt
  override def toInt(i: Int): Int = i
  override def toDouble(i: Int): Double = i
  override def leq(a: Int, b: Int): Boolean = a <= b
  override def getOrdering: Ordering[Int] = scala.math.Ordering.Int
  override def getMin(array : Array[Int]) : Int = array.min
  override def getMax(array : Array[Int]) : Int = array.max
  override def min(a : Int, b : Int) : Int = if(a < b) a else b
  override def isMinOrEqual(a : Int, b : Int) : Boolean = a <= b
  override def max(a : Int, b : Int) : Int = if(a > b) a else b

  override def mult[U](v : Int, v2 : U, typeInfo : TypeInfo[U]) : Int = {
    (typeInfo.toDouble(v2) * v).toInt
  }

  override def isValidForBIN(value : Int) : Boolean = !isNOTvalidDefined || (NaN match {
    case 0 => value != NaN
    case _ => !value.isNaN
  })

  override def getRealCut(cut: Int, info: Array[Int]) = {
    if (cut < 0)
      0.toShort
    else if ((cut-1) >= info.size)
      Short.MaxValue
    else {
      info(cut - 1)
    }
  }

  override def getSimpleSplit[U](min : Int, max : Int, binNumber : Int, typeInfo: TypeInfo[U]) : (Int => U) = {
    val binSize = (max-min) / binNumber
    (value : Int) => {
      if(isNOTvalidDefined && value == NaN) typeInfo.zero else
        typeInfo.fromInt(Math.min(binNumber - 1, Math.max(1, Math.ceil((value - (min + binSize)) / binSize))).toInt)
    }
  }

  override def getSimpleSplitInverted[U](min : Int, max : Int, binNumber : Int, typeInfo: TypeInfo[U]) : (U => Int) = {
    val binSize = ((max-min) / binNumber)
    (value : U) => {
      (min + (mult(binSize, value, typeInfo)))
    }
  }

  override def getIndex(array: Array[Int], value: Int): Int = {
    java.util.Arrays.binarySearch(array, value)
  }
}