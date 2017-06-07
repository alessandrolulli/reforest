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

package reforest.data

import Jama.QRDecomposition
import org.apache.commons.math3.random.MersenneTwister
import reforest.TypeInfo

import scala.reflect.ClassTag

/**
  * A rotation matrix constructed according to Blaser et al. "Random rotation ensembles"
  * @param n the size of the nxn matrix (n should be equals to the number of features in the dataset)
  * @param seed a random generator seed
  * @param input an optional matrix with random sampled values. If it is defined this is used instead of generating the number
  *              according to the Mersenne Twister (it should be used only for testing if the rotation matrix is correctly generated)
  */
class RotationMatrix(n: Int, seed : Int, input: Option[Array[Array[Double]]] = Option.empty) extends Serializable {

  private val matrix = generateRotationMatrix(input, seed)

  /**
    * Returns the generated matrix
    * @return the generated matrix
    */
  def getGeneratedMatrix = matrix

  /**
    * It rotates an array of values using the rotation matrix
    * @param array the array that must be rotated
    * @param typeInfo the type information of the raw data
    * @tparam T raw data type
    * @return the rotated array
    */
  def rotate[T: ClassTag](array: Array[T], typeInfo: TypeInfo[T]): Array[T] = {
    val toReturn: Array[T] = new Array[T](array.length)

    var i = 0
    while (i < matrix.length) {
      var j = 0
      var count = 0d
      while (j < matrix(i).length) {
        count += (matrix(i)(j) * typeInfo.toDouble(array(j)))
        j += 1
      }
      toReturn(i) = typeInfo.fromDouble(count)
      i += 1
    }

    toReturn
  }

  private def multInPlaceWithDiagonal(a : Array[Array[Double]], b : Array[Array[Double]]) = {
    var i = 0
    while(i < a.length) {
      var j = 0
      while(j < a(i).length) {
        a(i)(j) = a(i)(j) * b(j)(j)
        j += 1
      }
      i += 1
    }
    a
  }

  private def generateRotationMatrix(input: Option[Array[Array[Double]]] = Option.empty, seed : Int = 0) = {
    val randomGenerator = new MersenneTwister(seed)

    val m: Array[Array[Double]] = if (input.isDefined) input.get else Array.tabulate(n)(_ => Array.fill(n)(randomGenerator.nextDouble()))
    val qr = new QRDecomposition(new Jama.Matrix(m))

    val q = qr.getQ()
    val r = qr.getR()
    for (i <- 0 to n - 1) {
      for (j <- 0 to n - 1) {
        if (i == j) {
          if (r.get(i, j) > 0)
            r.set(i, j, 1)
          else if (r.get(i, j) < 0)
            r.set(i, j, -1)
          else r.set(i, j, 0)
        } else {
          r.set(i, j, 0)
        }
      }

    }
    val m2 = q.times(r)

    //      if(m2.det() < 0) {
    //        for(i <- 0 to n - 1) m2.set(i, 0, -m2.get(i,0))
    //      }

    m2.getArray()
  }
}
