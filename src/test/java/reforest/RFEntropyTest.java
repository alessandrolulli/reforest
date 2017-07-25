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
import reforest.data.tree.CutDetailed;
import reforest.rf.RFCategoryInfoEmpty;
import reforest.rf.RFEntropy;
import reforest.rf.feature.RFFeatureSizer;
import reforest.rf.feature.RFFeatureSizerSimple;
import reforest.rf.split.RFSplitter;
import reforest.test.BroadcastSimple;
import test.RFResourceFactory;

public class RFEntropyTest {

    private final int[][] data = {{0, 0}, {10, 0}, {5, 0}, {0, 20}, {0, 0}};
    private final int numClasses = 2;
    private final int numberBin = 32;
    private final RFSplitter<Double, Integer> splitter = RFResourceFactory.getSplitterRandomDefault(-23.5, 12.7, numberBin);


    @Test
    public void getBestSplit() {

        RFFeatureSizer featureSizer = new RFFeatureSizerSimple(32, 2, new RFCategoryInfoEmpty());
        RFEntropy<Double, Integer> entropy = new RFEntropy(BroadcastSimple.typeInfoDouble(), BroadcastSimple.typeInfoInt());

        CutDetailed<Double, Integer> cut = entropy.getBestSplit(data, 1, splitter, featureSizer, 3, 10, numClasses);

        Assert.assertEquals(2, BroadcastSimple.typeInfoInt().value().toInt(cut.bin()));
        Assert.assertEquals(15, cut.left());
        Assert.assertEquals(15, cut.labelLeftOk());
        Assert.assertEquals(20, cut.right());
        Assert.assertEquals(20, cut.labelRightOk());
        Assert.assertEquals(1, cut.label().get());
        Assert.assertEquals(0, cut.labelLeft().get());
        Assert.assertEquals(1, cut.labelRight().get());
        Assert.assertEquals(0, cut.notValid());
    }

    @Test
    public void getBestSplitCategorical() {

        RFEntropy<Double, Integer> entropy = new RFEntropy(BroadcastSimple.typeInfoDouble(), BroadcastSimple.typeInfoInt());

        CutDetailed<Double, Integer> cut = entropy.getBestSplitCategorical(data, 1, splitter, 3, 10, numClasses);

        Assert.assertEquals(3, BroadcastSimple.typeInfoInt().value().toInt(cut.bin()));
        Assert.assertEquals(20, cut.left());
        Assert.assertEquals(20, cut.labelLeftOk());
        Assert.assertEquals(15, cut.right());
        Assert.assertEquals(15, cut.labelRightOk());
        Assert.assertEquals(1, cut.label().get());
        Assert.assertEquals(1, cut.labelLeft().get());
        Assert.assertEquals(0, cut.labelRight().get());
        Assert.assertEquals(0, cut.notValid());
    }

}
