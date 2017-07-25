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
package reforest.rf;

import org.junit.Test;
import reforest.rf.feature.RFFeatureSizer;
import reforest.rf.feature.RFFeatureSizerSimpleModelSelection;
import reforest.rf.feature.RFFeatureSizerSpecializedModelSelection;
import reforest.rf.split.RFSplitter;
import test.RFResourceFactory;

import static org.junit.Assert.assertEquals;

public class RFFeatureSizerTest {
    private final int numberBin = 32;
    private final int numClasses = 10;
    private final RFSplitter<Double, Integer> splitter = RFResourceFactory.getSplitterRandomDefault(-23.5, 12.7, numberBin);
    private final RFFeatureSizer sizer = splitter.generateRFSizer(numClasses);

    @Test
    public void getSize(){
        assertEquals((numberBin + 1)*numClasses, sizer.getSize(1));
    }

    @Test
    public void shrinker() {
        RFFeatureSizerSimpleModelSelection sizer = new RFFeatureSizerSimpleModelSelection(32, 2, new RFCategoryInfoEmpty(), 16);

        assertEquals(0, sizer.getShrinkedValue(1, 0));
        assertEquals(1, sizer.getShrinkedValue(1, -1));
        assertEquals(16, sizer.getShrinkedValue(1, 32));
        assertEquals(16, sizer.getShrinkedValue(1, 33));
        assertEquals(16, sizer.getShrinkedValue(1, 100));
    }
}
