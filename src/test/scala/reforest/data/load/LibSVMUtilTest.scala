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

package reforest.data.load

import org.junit.{Assert, Test}
import reforest.test.BroadcastSimple

class LibSVMUtilTest {
  val util = new LibSVMUtil[Double, Integer](BroadcastSimple.typeInfoDouble,
    BroadcastSimple.gcInstrumentedEmpty,
    BroadcastSimple.categoryInfoEmpty);

  @Test
  def parseLibSVMRecordSparse(): Unit = {
    val returned : (Double, Array[Int], Array[Double]) = util.parseLibSVMRecord("1 1:2596.000000 20:51.000000 37:3.000000")

    Assert.assertEquals(1, returned._1, 0);
    Assert.assertEquals(3, returned._2.length);
    Assert.assertEquals(0, returned._2(0));
    Assert.assertEquals(19, returned._2(1));
    Assert.assertEquals(36, returned._2(2));
    Assert.assertEquals(3, returned._3.length);
    Assert.assertEquals(2596, returned._3(0),0);
    Assert.assertEquals(51, returned._3(1),0);
    Assert.assertEquals(3, returned._3(2),0);
  }

  @Test
  def parseLibSVMRecordDense(): Unit = {
    val returned : (Double, Array[Int], Array[Double]) = util.parseLibSVMRecord("1 1:2596.000000 2:51.000000 3:3.000000")

    Assert.assertEquals(1, returned._1, 0);
    Assert.assertEquals(3, returned._2.length);
    Assert.assertEquals(0, returned._2(0));
    Assert.assertEquals(1, returned._2(1));
    Assert.assertEquals(2, returned._2(2));
    Assert.assertEquals(3, returned._3.length);
    Assert.assertEquals(2596, returned._3(0),0);
    Assert.assertEquals(51, returned._3(1),0);
    Assert.assertEquals(3, returned._3(2),0);
  }
}
