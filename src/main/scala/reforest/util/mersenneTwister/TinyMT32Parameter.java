
package reforest.util.mersenneTwister;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;

/**
 * This class is used to keep parameters for TinyMT32, and to get parameters
 * from resource file.
 * 
 * @author M. Saito
 * 
 */
final class TinyMT32Parameter {
    /**
     * default parameter.
     */
    private static final TinyMT32Parameter DEFAULT_PARAMETER 
        = new TinyMT32Parameter(
            "d8524022ed8dff4a8dcc50c798faba43", 0, 0x8f7011ee, 0xfc78ff1f,
            0x3793fdff, 63, 0);
    /**
     * parameters for ThreadLocalRandom.
     */
    private static final TinyMT32Parameter[] THREAD_LOCAL_PARAMETER = {
            new TinyMT32Parameter("80227acb382d7b47f3714bd1223bedaf", 1,
                    0xda251b45, 0xfed0ffb5, 0x9b5cf7ff, 67, 0),
            new TinyMT32Parameter("db46f27d546507bdf3445acd188fa8a3", 1,
                    0xa55a14aa, 0xfd28ff4b, 0xc2b9efff, 67, 0),
            new TinyMT32Parameter("e1c47f40863c844be54fc078750562ef", 1,
                    0xa45b148a, 0xfd20ff49, 0x79aff7ff, 61, 0),
            new TinyMT32Parameter("d1346cadec1fbc329d1fe2283a577b77", 1,
                    0x837c106e, 0xfc18ff07, 0x5fffa9bf, 71, 0),
            new TinyMT32Parameter("c7487f9b2e8f8aaa231ac4b22b14db9b", 1,
                    0x817e102e, 0xfc08ff03, 0x69f3f77f, 65, 0),
            new TinyMT32Parameter("b0d7d986ce26326dbb6b0fccda28bdbb", 1,
                    0x68970d13, 0xfb40fed1, 0xd868edff, 71, 0),
            new TinyMT32Parameter("c0edefb49baf0424fd235ce48e8d26fb", 1,
                    0x45ba08b6, 0xfa28fe8b, 0x9c503dff, 69, 0),
            new TinyMT32Parameter("b24aa41c9eba05c4ffa8fc0e90438c37", 1,
                    0x2cd3059b, 0xf960fe59, 0xe67d73ff, 61, 0),
            new TinyMT32Parameter("a6aca2283f3e12ed69818bd95c35d00b", 1,
                    0x19e6033d, 0xf8c8fe33, 0x207e65ff, 61, 0),
            new TinyMT32Parameter("a86f5f29166785b075c431b027044287", 1,
                    0xfa041f41, 0xf7d8fdf7, 0x9c3efdff, 57, 0),
            new TinyMT32Parameter("e570ac318b37d9caa71ba4b99bad6a3b", 1,
                    0xf40a1e80, 0xf7a8fdeb, 0xe1cffbfd, 69, 0),
            new TinyMT32Parameter("d227f7cbfb9408765dd7da7d51790a13", 1,
                    0xf10f1e20, 0xf780fde1, 0xcac37fff, 71, 0),
            new TinyMT32Parameter("833689351c2ed91b5f56d10bde4b5197", 1,
                    0xdb251b65, 0xf6d0fdb5, 0x17bd7bff, 65, 0),
            new TinyMT32Parameter("dd68340e3a7a7b8d993c6f412b125ca7", 1,
                    0xc23c1846, 0xf618fd87, 0x899e7dff, 65, 0),
            new TinyMT32Parameter("9898245c4fccabd1617bb16fff089643", 1,
                    0xae5015cb, 0xf578fd5f, 0x93fc9ffd, 65, 1),
            new TinyMT32Parameter("e04a99bcc12b07a661440bef54402207", 1,
                    0x926c124c, 0xf498fd27, 0x01bbff5f, 53, 0) };
    /** int to float mask. */
    private static final int INT_TO_FLOAT_MASK = 0x3f800000;
    /** int to float shift. */
    private static final int INT_TO_FLOAT_SHIFT = 9;
    /** hexadecimal format. */
    private static final int HEX_FORMAT = 16;
    /** decimal format. */
    private static final int DEC_FORMAT = 10;
    /** characteristic polynomial. */
    private final F2Polynomial characteristic;
    /** ID of TinyMT32. */
    private final int id;
    /** parameter mat1 of TinyMT32. */
    private final int mat1;
    /** parameter mat2 of TinyMT32. */
    private final int mat2;
    /** parameter tmat of TinyMT32. */
    private final int tmat;
    /** Hamming weight of characteristic polynomial. */
    private final int weight;
    /** Delta of TinyMT. */
    private final int delta;

    /**
     * private constructor.
     * 
     * @param pcharacteristic
     *            hexadecimal format of characteristic polynomial
     * @param pid
     *            parameter ID
     * @param pmat1
     *            parameter mat1
     * @param pmat2
     *            parameter mat2
     * @param ptmat
     *            parameter tmat
     * @param pweight
     *            parameter weight
     * @param pdelta
     *            parameter delta
     */
    private TinyMT32Parameter(final String pcharacteristic, final int pid,
            final int pmat1, final int pmat2, final int ptmat,
            final int pweight, final int pdelta) {
        this.characteristic = new F2Polynomial(pcharacteristic, HEX_FORMAT);
        this.id = pid;
        this.mat1 = pmat1;
        this.mat2 = pmat2;
        this.tmat = ptmat;
        this.weight = pweight;
        this.delta = pdelta;
    }

    /**
     * returns default parameter of TinyMT32.
     * 
     * @return default parameter
     */
    static TinyMT32Parameter getDefaultParameter() {
        return DEFAULT_PARAMETER;
    }

    /**
     * returns default parameter for ThreadLocalRandom. This parameter is
     * calculated by TinyMT32DC with ID=1.
     * 
     * @param threadId thread ID
     * @return TinyMT32Parameter of ID=1
     */
    static TinyMT32Parameter getThreadLocalParameter(final long threadId) {
        int length = THREAD_LOCAL_PARAMETER.length;
        int no = (int) (threadId % length);
        if (no < 0) {
            no = -no;
        }
        return THREAD_LOCAL_PARAMETER[no];
    }

    /**
     * returns specified number of parameters.
     * 
     * @param count
     *            number of parameters to be returned
     * @return specified number of parameters
     * @throws IOException
     *             when fails to read resource file
     */
    static TinyMT32Parameter[] getParameters(final int count)
            throws IOException {
        return getParameters(0, count);
    }

    /**
     * returns if line is comment or default parameter.
     * 
     * @param line
     *            line from resource file
     * @return to be skipped or not.
     */
    private static boolean isSkip(final String line) {
        return line.startsWith("#")
                || line.startsWith("d8524022ed8dff4a8dcc50c798faba43");
    }

    /**
     * returns specified number of parameters from start line.
     * 
     * @param start
     *            line no. to start to read. 0 is first parameter except default
     *            parameter.
     * @param count
     *            number of parameters to be returned
     * @return specified number of parameters
     * @throws IOException
     *             when fails to read resource file
     */
    static TinyMT32Parameter[] getParameters(final int start, final int count)
            throws IOException {
        URL url = TinyMT32.class.getClassLoader().getResource(
                "tinymt32dc.0.65536.txt");
        ArrayList<TinyMT32Parameter> params 
            = new ArrayList<TinyMT32Parameter>();
        BufferedReader br = new BufferedReader(new InputStreamReader(
                url.openStream()));
        int index = 0;
        int preIdx = 0;
        try {
            while (start > preIdx) {
                String line = br.readLine();
                if (line == null) {
                    break;
                }
                if (isSkip(line)) {
                    continue;
                }
                preIdx++;
            }
            while (index < count) {
                String line = br.readLine();
                if (line == null) {
                    break;
                }
                if (isSkip(line)) {
                    continue;
                }
                TinyMT32Parameter p = parseString(line);
                params.add(p);
                index++;
            }
        } catch (IOException e) {
            if (index > 0) {
                return params.toArray(new TinyMT32Parameter[params.size()]);
            } else {
                throw e;
            }
        } finally {
            br.close();
        }
        return params.toArray(new TinyMT32Parameter[params.size()]);
    }

    /**
     * make TinyMT32Prameter instance from line.
     * 
     * @param line
     *            line of resource file
     * @return TinyMT32Parameter instance made from line
     * @throws IOException
     *             when line does not contain parameters.
     */
    private static TinyMT32Parameter parseString(final String line)
            throws IOException {
        String[] str = line.split(",");
        if (str.length < 8) {
            throw new IOException("line does not contain parameters.");
        }
        String characteristic = str[0];
        int id = Integer.parseInt(str[2]);
        int mat1 = (int) Long.parseLong(str[3], HEX_FORMAT);
        int mat2 = (int) Long.parseLong(str[4], HEX_FORMAT);
        int tmat = (int) Long.parseLong(str[5], HEX_FORMAT);
        int weight = Integer.parseInt(str[6], DEC_FORMAT);
        int delta = Integer.parseInt(str[7], DEC_FORMAT);
        return new TinyMT32Parameter(characteristic, id, mat1, mat2, tmat,
                weight, delta);
    }

    /**
     * return characteristic polynomial.
     * 
     * @return characteristic polynomial
     */
    F2Polynomial getCharacteristic() {
        return characteristic;
    }

    /**
     * return ID.
     * 
     * @return ID
     */
    int getId() {
        return id;
    }

    /**
     * return mat1.
     * 
     * @return mat1
     */
    int getMat1() {
        return mat1;
    }

    /**
     * return mat1 when x is odd number.
     * 
     * @param x
     *            number
     * @return mat1 when x is odd else 0
     */
    int getMat1(final int x) {
        // return ar1[x & 1];
        if ((x & 1) == 0) {
            return 0;
        } else {
            return mat1;
        }
    }

    /**
     * return mat2.
     * 
     * @return mat2
     */
    int getMat2() {
        return mat2;
    }

    /**
     * return mat2 when x is odd number.
     * 
     * @param x
     *            integer
     * @return mat1 if x is odd else 0
     */
    int getMat2(final int x) {
        // return ar2[x & 1];
        if ((x & 1) == 0) {
            return 0;
        } else {
            return mat2;
        }
    }

    /**
     * return tmat parameter.
     * 
     * @return tmat
     */
    int getTmat() {
        return tmat;
    }

    /**
     * return tmat if x is odd number.
     * 
     * @param x
     *            integer
     * @return return tmat if x is odd else 0
     */
    int getTmat(final int x) {
        // return art[x & 1];
        if ((x & 1) == 0) {
            return 0;
        } else {
            return tmat;
        }
    }

    /**
     * return bit pattern depends on x is odd or not.
     * 
     * @param x
     *            integer
     * @return bit pattern depends on x is odd or not.
     */
    int getTmatFloat(final int x) {
        // return arf[x & 1];
        if ((x & 1) == 0) {
            return INT_TO_FLOAT_MASK;
        } else {
            return INT_TO_FLOAT_MASK | (tmat >>> INT_TO_FLOAT_SHIFT);
        }
    }

    /**
     * return Hamming weight of characteristic polynomial.
     * 
     * @return Hamming weight of characteristic polynomial
     */
    int getWeight() {
        return weight;
    }

    /**
     * return delta.
     * 
     * @return delta
     */
    int getDelta() {
        return delta;
    }
}
