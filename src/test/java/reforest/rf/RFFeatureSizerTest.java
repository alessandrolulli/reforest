package reforest.rf;

import org.junit.Test;
import reforest.rf.split.RFSplitter;
import test.RFResourceFactory;

import static org.junit.Assert.assertEquals;

/**
 * Created by lulli on 6/7/17.
 */
public class RFFeatureSizerTest {
    private final int numberBin = 32;
    private final int numClasses = 10;
    private final RFSplitter<Double, Integer> splitter = RFResourceFactory.getSplitterRandomDefault(-23.5, 12.7, numberBin);
    private final RFFeatureSizer sizer = splitter.generateRFSizer(numClasses);

    @Test
    public void getSize(){
        assertEquals((numberBin + 1)*numClasses, sizer.getSize(1));
    }
}
