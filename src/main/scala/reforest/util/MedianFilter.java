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

package reforest.util;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class MedianFilter
{
    public static double getMean(double[] data_)
    {
        int sum = 0;
        for(double value_ : data_) sum += value_;
        return (double) sum / data_.length;
    }

    public static void main(String[] args_) throws Exception {
        String inputDATA = args_[0];
        int size = Integer.parseInt(args_[1]);
        String output = args_[2];

        final FileReader frDATA = new FileReader(inputDATA);
        final LineNumberReader lnrDATA = new LineNumberReader(frDATA);

        final FileOutputStream fileStreamOutput = new FileOutputStream(output, true);
        final PrintStream printOutput = new PrintStream(fileStreamOutput);

        String lineDATA;
        double[] mean = new double[size];
        int index = 0;
        int count = 0;

        while ((lineDATA = lnrDATA.readLine()) != null)
        {
            try {
                String[] token = lineDATA.split(",");
                mean[index] = Double.parseDouble(token[1]);

                printOutput.println(count + "," + getMean(mean)+ ","+token[0]);

                index = (index + 1) % mean.length;
                count++;
            }
            catch(Exception e)
            {

            }
        }

        printOutput.close();
        lnrDATA.close();
    }
}
