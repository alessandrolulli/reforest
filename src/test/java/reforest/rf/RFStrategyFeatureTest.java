package reforest.rf;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by lulli on 6/7/17.
 */
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

}
