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

package test;

import reforest.TypeInfo;
import reforest.TypeInfoDouble;
import reforest.TypeInfoInt;
import reforest.rf.RFCategoryInfo;
import reforest.rf.RFCategoryInfoEmpty;
import reforest.rf.split.RFSplitter;
import reforest.rf.split.RFSplitterSimpleRandom;
import reforest.rf.split.RFSplitterSpecialized;

import java.util.HashMap;
import java.util.Map;

public class RFResourceFactory {
    public static RFCategoryInfo getCategoricalInfo = new RFCategoryInfoEmpty();
    public static TypeInfoDouble typeInfoDouble = new TypeInfoDouble(false, 0);
    public static TypeInfoInt typeInfoInt = new TypeInfoInt(false, 0);

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

    public static RFSplitter<Double, Integer> getSplitterSpecializedDefault() {
        double[] aDouble = {10d,20d,30d,40d,50d};
        HashMap<Integer, double[]> map = new HashMap<>();
        map.put(0, aDouble);
        map.put(1, aDouble);
        map.put(2, aDouble);

        return new RFSplitterSpecialized(map, typeInfoDouble, typeInfoInt, getCategoricalInfo);
    }
}
