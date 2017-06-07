package reforest.rf;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by lulli on 6/7/17.
 */
public class RFCategoryInfoTest {
    private final RFCategoryInfoSpecialized categoryInfoALL = new RFCategoryInfoSpecialized("1:0,2:1,3:2,4:3","-1:4");
    private final RFCategoryInfoSpecialized categoryInfoPARTIAL = new RFCategoryInfoSpecialized("1:0,2:1,3:2,4:3","1:4,3:5");

    @Test
    public void isCategorical() {
        Assert.assertEquals(true, categoryInfoALL.isCategorical(1));
        Assert.assertEquals(true, categoryInfoALL.isCategorical(2));
        Assert.assertEquals(true, categoryInfoALL.isCategorical(3));

        Assert.assertEquals(true, categoryInfoPARTIAL.isCategorical(1));
        Assert.assertEquals(false, categoryInfoPARTIAL.isCategorical(2));
        Assert.assertEquals(true, categoryInfoPARTIAL.isCategorical(3));
    }

    @Test
    public void getArity() {
        Assert.assertEquals(4, categoryInfoALL.getArity(1));
        Assert.assertEquals(4, categoryInfoALL.getArity(2));
        Assert.assertEquals(4, categoryInfoALL.getArity(3));

        Assert.assertEquals(4, categoryInfoPARTIAL.getArity(1));
        Assert.assertEquals(0, categoryInfoPARTIAL.getArity(2));
        Assert.assertEquals(5, categoryInfoPARTIAL.getArity(3));
    }

    @Test
    public void rawRemapping() {
        Assert.assertEquals(0, categoryInfoALL.rawRemapping(1));
        Assert.assertEquals(1, categoryInfoALL.rawRemapping(2));
        Assert.assertEquals(2, categoryInfoALL.rawRemapping(3));

        Assert.assertEquals(0, categoryInfoPARTIAL.rawRemapping(1));
        Assert.assertEquals(1, categoryInfoPARTIAL.rawRemapping(2));
        Assert.assertEquals(2, categoryInfoPARTIAL.rawRemapping(3));
    }
}
