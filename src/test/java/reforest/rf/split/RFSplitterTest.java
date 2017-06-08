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

package reforest.rf.split;

import reforest.rf.RFFeatureSizer;
import test.RFResourceFactory;
import org.junit.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class RFSplitterTest {

    private final int numberBin = 32;
    private final int numClasses = 10;
    private final RFSplitter<Double, Integer> splitter = RFResourceFactory.getSplitterRandomDefault(-23.5, 12.7, numberBin);

    @Test
    public void getBin() {

        assertEquals(1, (int) splitter.getBin(1, -25d));
        assertEquals(1, (int) splitter.getBin(1, -23.5d));
        assertEquals(numberBin, (int) splitter.getBin(1, 12.7d));
        assertEquals(numberBin, (int) splitter.getBin(1, 22.7d));

    }

    @Test
    public void getBinNumber() {

        assertEquals(numberBin + 1, (int) splitter.getBinNumber(1));

    }

    @Test
    public void getRealCut() {

        assertEquals(0d, splitter.getRealCut(1, -1));
        assertEquals(Double.MAX_VALUE, splitter.getRealCut(1, numberBin + numberBin));

    }

    @Test
    public void generateRFSizer() {

        RFFeatureSizer sizer = splitter.generateRFSizer(numClasses);

        assertNotNull(sizer);

    }

    @Test
    public void getBinSpecialized() {

        RFSplitter<Double, Integer> splitter = RFResourceFactory.getSplitterSpecializedDefault();
        // double[] aDouble = {10d,20d,30d,40d,50d};

        assertEquals(1, (int) splitter.getBin(1, -25d));
        assertEquals(1, (int) splitter.getBin(1, 0d));
        assertEquals(1, (int) splitter.getBin(1, 10d));
        assertEquals(2, (int) splitter.getBin(1, 10.1d));
        assertEquals(2, (int) splitter.getBin(1, 20d));
        assertEquals(5, (int) splitter.getBin(1, 48.5d));
        assertEquals(5, (int) splitter.getBin(1, 50d));
        assertEquals(6, (int) splitter.getBin(1, 50.5d));
        assertEquals(6, (int) splitter.getBin(1, 150.5d));

    }

    @Test
    public void getBinNumberSpecialized() {

        RFSplitter<Double, Integer> splitter = RFResourceFactory.getSplitterSpecializedDefault();

        assertEquals(7, (int) splitter.getBinNumber(1));

    }

    @Test
    public void getRealCutSpecialized() {

        RFSplitter<Double, Integer> splitter = RFResourceFactory.getSplitterSpecializedDefault();
        // double[] aDouble = {10d,20d,30d,40d,50d};

        assertEquals(0d, splitter.getRealCut(1, -1));
        assertEquals(10d, splitter.getRealCut(1, 1));
        assertEquals(20d, splitter.getRealCut(1, 2));
        assertEquals(30d, splitter.getRealCut(1, 3));
        assertEquals(40d, splitter.getRealCut(1, 4));
        assertEquals(50d, splitter.getRealCut(1, 5));
        assertEquals(Double.MAX_VALUE, splitter.getRealCut(1, 6));
        assertEquals(Double.MAX_VALUE, splitter.getRealCut(1, numberBin + numberBin));

    }

    @Test
    public void generateRFSizerSpecialized() {

        RFSplitter<Double, Integer> splitter = RFResourceFactory.getSplitterSpecializedDefault();
        // double[] aDouble = {10d,20d,30d,40d,50d};

        RFFeatureSizer sizer = splitter.generateRFSizer(numClasses);

        assertNotNull(sizer);
        assertEquals(7*numClasses, sizer.getSize(1));

    }
}
