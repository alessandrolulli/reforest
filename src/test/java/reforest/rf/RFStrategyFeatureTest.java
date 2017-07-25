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

import org.junit.Assert;
import org.junit.Test;
import reforest.rf.feature.*;

public class RFStrategyFeatureTest {

    private final int featureNumber = 25;

    @Test
    public void strategyALL() {
        RFStrategyFeatureALL strategy = new RFStrategyFeatureALL(featureNumber);

        Assert.assertEquals(featureNumber, strategy.getFeatureNumber());
        Assert.assertEquals(featureNumber, strategy.getFeaturePerNodeNumber());
    }

    @Test
    public void strategyONETHIRD() {
        RFStrategyFeatureONETHIRD strategy = new RFStrategyFeatureONETHIRD(featureNumber);

        Assert.assertEquals(featureNumber, strategy.getFeatureNumber());
        Assert.assertEquals(Math.ceil((double)featureNumber / 3), strategy.getFeaturePerNodeNumber(), 0.1);
    }

    @Test
    public void strategySQRT() {
        RFStrategyFeatureSQRT strategy = new RFStrategyFeatureSQRT(featureNumber);

        Assert.assertEquals(featureNumber, strategy.getFeatureNumber());
        Assert.assertEquals(Math.ceil(Math.sqrt(featureNumber)), strategy.getFeaturePerNodeNumber(), 0.1);
    }

    @Test
    public void strategySQRTSQRT() {
        RFStrategyFeatureSQRTSQRT strategy = new RFStrategyFeatureSQRTSQRT(featureNumber);

        Assert.assertEquals(featureNumber, strategy.getFeatureNumber());
        Assert.assertEquals(Math.ceil(Math.sqrt(Math.sqrt(featureNumber))), strategy.getFeaturePerNodeNumber(), 0.1);
    }

    @Test
    public void strategyLOG2() {
        RFStrategyFeatureLOG2 strategy = new RFStrategyFeatureLOG2(featureNumber);

        Assert.assertEquals(featureNumber, strategy.getFeatureNumber());
        Assert.assertEquals(Math.ceil(Math.log(featureNumber) / Math.log(2)), strategy.getFeaturePerNodeNumber(), 0.1);
    }

}
