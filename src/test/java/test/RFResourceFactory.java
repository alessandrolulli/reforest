package test;

import reforest.TypeInfo;
import reforest.TypeInfoByte;
import reforest.TypeInfoDouble;
import reforest.TypeInfoInt;
import reforest.rf.RFCategoryInfo;
import reforest.rf.RFCategoryInfoEmpty;
import reforest.rf.split.RFSplitter;
import reforest.rf.split.RFSplitterSimpleRandom;
import scala.tools.cmd.gen.AnyVals;

/**
 * Created by lulli on 6/7/17.
 */
public class RFResourceFactory {
    public static RFCategoryInfo getCategoricalInfo = new RFCategoryInfoEmpty();

    public static <T, U> RFSplitter<T, U> getSplitterRandom(T min, T max, TypeInfo<T> typeInfo, TypeInfo<U> typeInfoWorking, int numberBin) {
        return new RFSplitterSimpleRandom<T, U>(min, max,
                typeInfo,
                typeInfoWorking,
                numberBin,
                getCategoricalInfo);
    }

    public static RFSplitter<Double, Integer> getSplitterRandomDefault(double min,
                                                                    double max,
                                                                    int numberBin) {
        RFSplitter a =  getSplitterRandom(min,
                max,
                new TypeInfoDouble(false, 0),
                new TypeInfoInt(false, 0),
                numberBin);
        return a;
    }
}
