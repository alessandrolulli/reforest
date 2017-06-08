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

/**
  * Utilities to work with a generic type.
  * These utilities are used to optimize the data structure to load the raw data and to store the working data.
  * @tparam T The type of the data. Available {Byte, Short, Int, Float, Double}
  */
trait TypeInfo[T] extends Serializable {
  /**
    * The representation of zero
    * @return the zero value
    */
  def zero : T

  /**
    * The value representing NaN (not a number)
    * @return the NaN value
    */
  def NaN: T

  /**
    * The minimum allowed value
    * @return the minimum allowed value
    */
  def minValue : T

  /**
    * The maximum allowed value
    * @return the maximum allowed value
    */
  def maxValue : T

  /**
    * Check if the value passed as argument is a valid value for performing binning
    * @param value the value to check
    * @return true if the value is a valid value for performing binning
    */
  def isValidForBIN(value : T) : Boolean

  /**
    * Check if the value not valid is defined
    * @return true if the value not valid is defined
    */
  def isNOTvalidDefined : Boolean

  /**
    * Convert the string value passed as parameter to T
    * @param s The value to be converted
    * @return The converted value
    */
  def fromString(s: String): T

  /**
    * Convert the int value passed as parameter to T
    * @param i The value to be converted
    * @return The converted value
    */
  def fromInt(i: Int): T

  /**
    * Convert the double value passed as parameter to T
    * @param i The value to be converted
    * @return The converted value
    */
  def fromDouble(i: Double): T

  /**
    * Convert a value in T to Int
    * @param i The value to be converted
    * @return The converted value
    */
  def toInt(i: T): Int

  /**
    * Convert a value in T to Double
    * @param i The value to be converted
    * @return The converted value
    */
  def toDouble(i: T): Double

  /**
    * Perform a multiplication between two values of different types
    * @param i the first value
    * @param v the second value in a different type
    * @param typeInfo the type information for the second value
    * @tparam U the type of the second value
    * @return the result of the multiplication in type T
    */
  def mult[U](i: T, v : U, typeInfo: TypeInfo[U]) : T

  /**
    * Get the cut represented by the passed bin in type T
    * @param cut the bin number
    * @param split the array representing all the candidate cut
    * @return the cut in type T instead that the initial Int
    */
  def getRealCut(cut: Int, split: Array[T]): T

  /**
    * Find the index in the array passed as argument of the value passed as argument
    * @param array the array with all the intervals
    * @param value the value to search in the array
    * @return the index of the value in the array
    */
  def getIndex(array: Array[T], value: T): Int

  /**
    * The utility for ordering element in type T
    * @return the utility for ordering element in type T
    */
  def getOrdering : Ordering[T]

  /**
    * Find the minimum value in the RawData
    * @param data the raw data
    * @return the minimum value in the raw data
    */
  def getMin(data : RawData[T, _]) : T = {
    data match {
      case d : RawDataDense[T, _] => getMin(d.values)
      case d : RawDataSparse[T, _] => getMin(d.values)
      case _ => throw new ClassCastException
    }
  }

  /**
    * Find the minimum value in the Array
    * @param array the array
    * @return the minimum value in the array
    */
  def getMin(array : Array[T]) : T

  /**
    * Find the maximum value in the RawData
    * @param data the raw data
    * @return the maximum value in the raw data
    */
  def getMax(data : RawData[T, _]) : T = {
    data match {
      case d : RawDataDense[T, _] => getMax(d.values)
      case d : RawDataSparse[T, _] => getMax(d.values)
      case _ => throw new ClassCastException
    }
  }

  /**
    * Find the maximum value in the Array
    * @param array the array
    * @return the maximum value in the array
    */
  def getMax(array : Array[T]) : T

  /**
    * Find the min value between two values
    * @param a the first value
    * @param b the second value
    * @return the minimum value
    */
  def min(a : T, b : T) : T

  /**
    * Check if the first value is less or equal with respect to the second value
    * @param a the first value
    * @param b the second value
    * @return true if the first value is less or equal with respect to the second value
    */
  def isMinOrEqual(a : T, b : T) : Boolean

  /**
    * Find the max value between two values
    * @param a the first value
    * @param b the second value
    * @return the maximum value
    */
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

  override def getRealCut(cut: Int, info: Array[Double]) = {
    if (cut < 0)
      0d
    else if ((cut-1) >= info.size)
      Double.MaxValue
    else {
      info(cut - 1)
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

  override def getRealCut(cut: Int, info: Array[Float]) = {
    if (cut < 0)
      0f
    else if ((cut-1) >= info.size)
      Float.MaxValue
    else {
      info(cut - 1)
    }
  }

  override def getIndex(array: Array[Float], value: Float): Int = {
    java.util.Arrays.binarySearch(array, value)
  }
}

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

  override def getRealCut(cut: Int, info: Array[Byte]) = {
    if (cut < 0)
      0.toByte
    else if ((cut-1) >= info.size)
      Byte.MaxValue
    else {
      info(cut - 1)
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
      0
    else if ((cut-1) >= info.size)
      Int.MaxValue
    else {
      info(cut - 1)
    }
  }

  override def getIndex(array: Array[Int], value: Int): Int = {
    java.util.Arrays.binarySearch(array, value)
  }
}