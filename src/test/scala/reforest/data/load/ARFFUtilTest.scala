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

class ARFFUtilTest {
  val util = new ARFFUtil[Double, Integer](BroadcastSimple.typeInfoDouble,
    BroadcastSimple.gcInstrumentedEmpty,
    BroadcastSimple.categoryInfoEmpty);

  @Test
  def parseARFFRecord(): Unit = {
    val returned : (Double, Array[Double]) = util.parseARFFRecord("5.1,3.5,1.4,0.2,0")

    Assert.assertEquals(0, returned._1, 0);
    Assert.assertEquals(4, returned._2.length);
    Assert.assertEquals(5.1, returned._2(0),0.000001);
    Assert.assertEquals(3.5, returned._2(1),0.000001);
    Assert.assertEquals(1.4, returned._2(2),0.000001);
    Assert.assertEquals(0.2, returned._2(3),0.000001);
  }

}
