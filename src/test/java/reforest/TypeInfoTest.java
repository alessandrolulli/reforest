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

package reforest;

import org.junit.Assert;
import org.junit.Test;

public class TypeInfoTest {
    private final TypeInfoByte tByte = new TypeInfoByte(false, Byte.valueOf("0"));
    private final TypeInfoShort tShort = new TypeInfoShort(false, Short.valueOf("0"));
    private final TypeInfoInt tInt = new TypeInfoInt(false, 0);
    private final TypeInfoFloat tFloat = new TypeInfoFloat(false, 0);
    private final TypeInfoDouble tDouble = new TypeInfoDouble(false, 0);

    private final byte[] aByte = {10,20,30,40,50};
    private final short[] aShort = {10,20,30,40,50};
    private final int[] aInt = {10,20,30,40,50};
    private final float[] aFloat = {10,20,30,40,50};
    private final double[] aDouble = {10,20,30,40,50};

    @Test
    public void zero() {
        Assert.assertEquals(0, tByte.zero());
        Assert.assertEquals(0, tShort.zero());
        Assert.assertEquals(0, tInt.zero());
        Assert.assertEquals(0, tFloat.zero(), 0);
        Assert.assertEquals(0, tDouble.zero(), 0);
    }

    @Test
    public void minValue() {
        Assert.assertEquals(Byte.MIN_VALUE, tByte.minValue());
        Assert.assertEquals(Short.MIN_VALUE, tShort.minValue());
        Assert.assertEquals(Integer.MIN_VALUE, tInt.minValue());
        Assert.assertEquals(scala.Float.MinValue(), tFloat.minValue(), 0);
        Assert.assertEquals(scala.Double.MinValue(), tDouble.minValue(), 0);
    }

    @Test
    public void maxValue() {
        Assert.assertEquals(Byte.MAX_VALUE, tByte.maxValue());
        Assert.assertEquals(Short.MAX_VALUE, tShort.maxValue());
        Assert.assertEquals(Integer.MAX_VALUE, tInt.maxValue());
        Assert.assertEquals(Float.MAX_VALUE, tFloat.maxValue(), 0);
        Assert.assertEquals(Double.MAX_VALUE, tDouble.maxValue(), 0);
    }

    @Test
    public void fromString() {
        Assert.assertEquals(0, tByte.fromString("0"));
        Assert.assertEquals(0, tShort.fromString("0"));
        Assert.assertEquals(0, tInt.fromString("0"));
        Assert.assertEquals(0, tFloat.fromString("0"), 0);
        Assert.assertEquals(0, tDouble.fromString("0"), 0);
    }

    @Test
    public void fromInt() {
        Assert.assertEquals(0, tByte.fromInt(0));
        Assert.assertEquals(0, tShort.fromInt(0));
        Assert.assertEquals(0, tInt.fromInt(0));
        Assert.assertEquals(0, tFloat.fromInt(0), 0);
        Assert.assertEquals(0, tDouble.fromInt(0), 0);
    }

    @Test
    public void fromDouble() {
        Assert.assertEquals(0, tByte.fromDouble(0.2));
        Assert.assertEquals(0, tShort.fromDouble(0.2));
        Assert.assertEquals(0, tInt.fromDouble(0.2));
        Assert.assertEquals(0.2, tFloat.fromDouble(0.2), 0.000001);
        Assert.assertEquals(0.2, tDouble.fromDouble(0.2), 0.000001);
    }

    @Test
    public void getRealCut() {
        Assert.assertEquals(30, tByte.getRealCut(3, aByte));
        Assert.assertEquals(30, tShort.getRealCut(3, aShort));
        Assert.assertEquals(30, tInt.getRealCut(3, aInt));
        Assert.assertEquals(30, tFloat.getRealCut(3, aFloat), 0);
        Assert.assertEquals(30, tDouble.getRealCut(3, aDouble), 0);

        Assert.assertEquals(Byte.MAX_VALUE, tByte.getRealCut(20, aByte));
        Assert.assertEquals(Short.MAX_VALUE, tShort.getRealCut(20, aShort));
        Assert.assertEquals(Integer.MAX_VALUE, tInt.getRealCut(20, aInt));
        Assert.assertEquals(Float.MAX_VALUE, tFloat.getRealCut(20, aFloat), 0);
        Assert.assertEquals(Double.MAX_VALUE, tDouble.getRealCut(20, aDouble), 0);

        Assert.assertEquals(0, tByte.getRealCut(-1, aByte));
        Assert.assertEquals(0, tShort.getRealCut(-1, aShort));
        Assert.assertEquals(0, tInt.getRealCut(-1, aInt));
        Assert.assertEquals(0, tFloat.getRealCut(-1, aFloat), 0);
        Assert.assertEquals(0, tDouble.getRealCut(-1, aDouble), 0);
    }

    @Test
    public void getIndex() {
        Assert.assertEquals(-1, tByte.getIndex(aByte, Byte.valueOf("-5")));
        Assert.assertEquals(-1, tShort.getIndex(aShort, Short.valueOf("-5")));
        Assert.assertEquals(-1, tInt.getIndex(aInt, -5));
        Assert.assertEquals(-1, tFloat.getIndex(aFloat, -5));
        Assert.assertEquals(-1, tDouble.getIndex(aDouble, -5));

        Assert.assertEquals(-1, tByte.getIndex(aByte, Byte.valueOf("9")));
        Assert.assertEquals(-1, tShort.getIndex(aShort, Short.valueOf("9")));
        Assert.assertEquals(-1, tInt.getIndex(aInt, 9));
        Assert.assertEquals(-1, tFloat.getIndex(aFloat, 9));
        Assert.assertEquals(-1, tDouble.getIndex(aDouble, 9));

        Assert.assertEquals(-3, tByte.getIndex(aByte, Byte.valueOf("22")));
        Assert.assertEquals(-3, tShort.getIndex(aShort, Short.valueOf("22")));
        Assert.assertEquals(-3, tInt.getIndex(aInt, 22));
        Assert.assertEquals(-3, tFloat.getIndex(aFloat, 22));
        Assert.assertEquals(-3, tDouble.getIndex(aDouble, 22));

        Assert.assertEquals(-6, tByte.getIndex(aByte, Byte.valueOf("99")));
        Assert.assertEquals(-6, tShort.getIndex(aShort, Short.valueOf("99")));
        Assert.assertEquals(-6, tInt.getIndex(aInt, 99));
        Assert.assertEquals(-6, tFloat.getIndex(aFloat, 99));
        Assert.assertEquals(-6, tDouble.getIndex(aDouble, 99));
    }

}
