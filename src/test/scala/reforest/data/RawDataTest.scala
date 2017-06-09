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

import org.junit.{Assert, Test}

class RawDataTest {

  @Test
  def constructDense() = {

    val dataDense = RawData.dense[Double, Integer](Array(1, 2.3, 0.7, -5.23, 123.34), -100)

    Assert.assertEquals(5, dataDense.size)
    Assert.assertEquals(1, dataDense(0), 0.00001)
    Assert.assertEquals(-5.23, dataDense(3), 0.00001)
  }

  @Test
  def constructSparse() = {

    val dataSparse = RawData.sparse[Double, Integer](87, Array(0, 5, 10, 23, 24), Array(1, 2.3, 0.7, -5.23, 123.34), -100)

    Assert.assertEquals(87, dataSparse.size)
    Assert.assertEquals(1, dataSparse(0), 0.00001)
    Assert.assertEquals(-5.23, dataSparse(23), 0.00001)
    Assert.assertEquals(-100, dataSparse(55), 0.00001)
  }

  @Test
  def compressed() = {
    val dataDense = RawData.dense[Double, Integer](Array(1, 2.3, 0.7, -5.23, 123.34), -100)
    val dataSparse = RawData.sparse[Double, Integer](87, Array(0, 5, 10, 23, 24), Array(1, 2.3, 0.7, -5.23, 123.34), -100)
    val dataSparseCompressable = RawData.sparse[Double, Integer](5, Array(0, 1, 2, 3, 4), Array(1, 2.3, 0.7, -5.23, 123.34), -100)

    Assert.assertEquals(true, dataSparseCompressable.compressed match {case v : RawDataDense[Double,Integer] => true; case _ => false})
    Assert.assertEquals(5, dataSparseCompressable.compressed.size)

  }

  @Test
  def numActives() = {
    val dataDense = RawData.dense[Double, Integer](Array(1, 2.3, 0.7, -5.23, 123.34), -100)
    val dataSparse = RawData.sparse[Double, Integer](87, Array(0, 5, 10, 23, 24), Array(1, 2.3, 0.7, -5.23, 123.34), -100)

    Assert.assertEquals(5, dataDense.numActives)
    Assert.assertEquals(5, dataSparse.numActives)
  }

  @Test
  def numNonzeros() = {
    val dataDense = RawData.dense[Double, Integer](Array(1, 2.3, 0.7, -5.23, 123.34), -100)
    val dataSparse = RawData.sparse[Double, Integer](87, Array(0, 5, 10, 23, 24), Array(1, 2.3, 0.7, -5.23, 123.34), -100)

    Assert.assertEquals(5, dataDense.numNonzeros)
    Assert.assertEquals(5, dataSparse.numNonzeros)
  }

  @Test
  def toArray() = {
    val dataDense = RawData.dense[Double, Integer](Array(1, 2.3, 0.7, -5.23, 123.34), -100)
    val dataSparse = RawData.sparse[Double, Integer](87, Array(0, 5, 10, 23, 24), Array(1, 2.3, 0.7, -5.23, 123.34), -100)

    Assert.assertEquals(5, dataDense.toArray.length)
    Assert.assertEquals(87, dataSparse.toArray.length)

    Assert.assertEquals(-5.23, dataDense.toArray(3),0.000001)
    Assert.assertEquals(0, dataSparse.toArray(3),0.000001)
    Assert.assertEquals(-5.23, dataSparse.toArray(23),0.000001)
  }
}
