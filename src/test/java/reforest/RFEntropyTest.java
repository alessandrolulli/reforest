package reforest;

import org.junit.Assert;
import org.junit.Test;
import reforest.dataTree.CutDetailed;
import reforest.rf.RFEntropy;
import reforest.rf.split.RFSplitter;
import test.BroadcastSimple;
import test.RFResourceFactory;

/**
 * Created by lulli on 6/7/17.
 */
public class RFEntropyTest {

    private final int[][] data = {{0, 0}, {10, 0}, {5, 0}, {0, 20}, {0, 0}};
    private final int numClasses = 2;
    private final int numberBin = 32;
    private final RFSplitter<Double, Integer> splitter = RFResourceFactory.getSplitterRandomDefault(-23.5, 12.7, numberBin);


    @Test
    public void getBestSplit() {

        RFEntropy<Double, Integer> entropy = new RFEntropy(BroadcastSimple.typeInfoDouble(), BroadcastSimple.typeInfoInt());

        CutDetailed<Double, Integer> cut = entropy.getBestSplit(data, 1, splitter, 3, 10, numClasses);

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

}
