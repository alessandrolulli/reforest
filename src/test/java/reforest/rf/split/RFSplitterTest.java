package reforest.rf.split;

import reforest.rf.RFFeatureSizer;
import test.RFResourceFactory;
import org.junit.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;


/**
 * Created by lulli on 6/7/17.
 */
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
}
