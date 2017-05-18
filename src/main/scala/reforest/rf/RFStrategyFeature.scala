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

import java.nio.ByteBuffer
import java.util.Random


import scala.reflect.ClassTag
import scala.util.Random
import scala.util.hashing.MurmurHash3

trait RFStrategyFeature extends Serializable {
  def getDescription() : String
  def getFeatureNumber : Int
  def getFeaturePerNode : Array[Int]
  def getFeaturePerNodeNumber : Int
}

class RFStrategyFeatureALL(featureNumber : Int) extends RFStrategyFeature {
  val featurePerNode = Array.tabulate(featureNumber)(i => i)

  def getDescription() : String = "ALL"
  def getFeatureNumber : Int = featureNumber
  def getFeaturePerNode : Array[Int] = featurePerNode
  def getFeaturePerNodeNumber : Int = featureNumber
}

abstract class RFStrategyFeatureBase(featureNumber : Int) extends RFStrategyFeature {
  private val r = scala.util.Random

  def getFeatureNumber : Int = featureNumber
  def getFeaturePerNode : Array[Int] = {
    var i = 0
    var toReturn = Set[Int]()
    while (toReturn.size < getFeaturePerNodeNumber) {
      toReturn = toReturn + r.nextInt(getFeatureNumber)
    }
    toReturn.toArray
  }

//  def reservoirSampleAndCount[T: ClassTag](
//                                            input: Iterator[T],
//                                            k: Int,
//                                            seed: Long = scala.util.Random.nextLong())
//  : (Array[T], Long) = {
//    val reservoir = new Array[T](k)
//    // Put the first k elements in the reservoir.
//    var i = 0
//    while (i < k && input.hasNext) {
//      val item = input.next()
//      reservoir(i) = item
//      i += 1
//    }
//
//    // If we have consumed all the elements, return them. Otherwise do the replacement.
//    if (i < k) {
//      // If input size < k, trim the array to return only an array of input size.
//      val trimReservoir = new Array[T](i)
//      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
//      (trimReservoir, i)
//    } else {
//      // If input size > k, continue the sampling process.
//      var l = i.toLong
//      val rand = new XORShiftRandom(seed)
//      while (input.hasNext) {
//        val item = input.next()
//        val replacementIndex = (rand.nextDouble() * l).toLong
//        if (replacementIndex < k) {
//          reservoir(replacementIndex.toInt) = item
//        }
//        l += 1
//      }
//      (reservoir, l)
//    }
//  }
}

class RFStrategyFeatureSQRT(featureNumber : Int) extends RFStrategyFeatureBase(featureNumber) {
  override def getDescription() : String = "SQRT"
  override def getFeaturePerNodeNumber : Int = Math.sqrt(featureNumber).ceil.toInt
}

class RFStrategyFeatureSQRTSQRT(featureNumber : Int) extends RFStrategyFeatureBase(featureNumber) {
  override def getDescription() : String = "SQRTSQRT"
  override def getFeaturePerNodeNumber : Int = Math.sqrt(Math.sqrt(featureNumber)).ceil.toInt
}

class RFStrategyFeatureLOG2(featureNumber : Int) extends RFStrategyFeatureBase(featureNumber) {
  override def getDescription() : String = "LOG2"
  override def getFeaturePerNodeNumber : Int = (scala.math.log(featureNumber) / scala.math.log(2)).ceil.toInt
}

class RFStrategyFeatureONETHIRD(featureNumber : Int) extends RFStrategyFeatureBase(featureNumber) {
  override def getDescription() : String = "ONETHIRD"
  override def getFeaturePerNodeNumber : Int = (featureNumber.toDouble / 3).ceil.toInt
}

//class XORShiftRandom(init: Long) extends java.util.Random(init) {
//
//  def this() = this(System.nanoTime)
//
//  private var seed = XORShiftRandom.hashSeed(init)
//
//  // we need to just override next - this will be called by nextInt, nextDouble,
//  // nextGaussian, nextLong, etc.
//  override protected def next(bits: Int): Int = {
//    var nextSeed = seed ^ (seed << 21)
//    nextSeed ^= (nextSeed >>> 35)
//    nextSeed ^= (nextSeed << 4)
//    seed = nextSeed
//    (nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
//  }
//
//  override def setSeed(s: Long) {
//    seed = XORShiftRandom.hashSeed(s)
//  }
//}
//
///** Contains benchmark method and main method to run benchmark of the RNG */
//object XORShiftRandom {
//
//  /** Hash seeds to have 0/1 bits throughout. */
//  def hashSeed(seed: Long): Long = {
//    val bytes = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(seed).array()
//    val lowBits = MurmurHash3.bytesHash(bytes)
//    val highBits = MurmurHash3.bytesHash(bytes, lowBits)
//    (highBits.toLong << 32) | (lowBits.toLong & 0xFFFFFFFFL)
//  }
//
//  /**
//    * Main method for running benchmark
//    * @param args takes one argument - the number of random numbers to generate
//    */
//  def main(args: Array[String]): Unit = {
//    // scalastyle:off println
//    if (args.length != 1) {
//      println("Benchmark of XORShiftRandom vis-a-vis java.util.Random")
//      println("Usage: XORShiftRandom number_of_random_numbers_to_generate")
//      System.exit(1)
//    }
//
//  }
//
//
//}
