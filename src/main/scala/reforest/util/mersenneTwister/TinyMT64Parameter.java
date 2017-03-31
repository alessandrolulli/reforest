

package reforest.util.mersenneTwister;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;

/**
 * This class is used to keep parameters for TinyMT64, and to get parameters
 * from resource file.
 * @author M. Saito
 *
 */
final class TinyMT64Parameter {
    /** default parameter. */
    private static final TinyMT64Parameter DEFAULT_PARAMETER 
        = new TinyMT64Parameter(
            "945e0ad4a30ec19432dfa9d5959e5d5d", 0, 0xfa051f40, 0xffd0fff4,
            0x58d02ffeffbfffbcL, 65, 0);
    /** long to double mask. */
    private static final long LONG_TO_DOUBLE_MASK = 0x3ff0000000000000L;
    /** int to unsigned long mask. */
    private static final long INT_TO_LONG_MASK = 0xffffffffL;
    /** long to double shift. */
    private static final int LONG_TO_DOUBLE_SHIFT = 12;
    /** hexadecimal format. */
    private static final int HEX_FORMAT = 16;
    /** decimal format. */
    private static final int DEC_FORMAT = 10;
    /** long bit size. */
    private static final int LONG_BIT_SIZE = 64;
    /** characteristic polynomial. */
    private final F2Polynomial characteristic;
    /** ID of TinyMT32. */
    private final int id;
    /** parameter mat1 of TinyMT64. */
    private final int mat1;
    /** parameter mat2 of TinyMT64. */
    private final int mat2;
    /** parameter tmat of TinyMT64. */
    private final long tmat;
    /** Hamming weight of characteristic polynomial. */
    private final int weight;
    /** Delta of TinyMT. */
    private final int delta;

    /**
     * private constructor.
     * @param pcharacteristic hexadecimal format of characteristic polynomial
     * @param pid parameter ID
     * @param pmat1 parameter mat1 
     * @param pmat2 parameter mat2
     * @param ptmat parameter tmat
     * @param pweight parameter weight
     * @param pdelta parameter delta
     */
    private TinyMT64Parameter(final String pcharacteristic,
            final int pid, final int pmat1, final int pmat2,
            final long ptmat, final int pweight, final int pdelta) {
        this.characteristic = new F2Polynomial(pcharacteristic, HEX_FORMAT);
        this.id = pid;
        this.mat1 = pmat1;
        this.mat2 = pmat2;
        this.tmat = ptmat;
        this.weight = pweight;
        this.delta = pdelta;
    }

    /**
     * returns default parameter of TinyMT64.
     * @return default parameter
     */
    static TinyMT64Parameter getDefaultParameter() {
        return DEFAULT_PARAMETER;
    }

    /**
     * returns specified number of parameters.
     * @param count number of parameters to be returned
     * @return specified number of parameters
     * @throws IOException when fails to read resource file
     */
    static TinyMT64Parameter[] getParameters(final int count)
            throws IOException {
    return getParameters(0, count);
    }   

    /**
     * returns if line is comment or default parameter.
     * @param line line from resource file
     * @return to be skipped or not.
     */
    private static boolean isSkip(final String line) {
        return line.startsWith("#")
                || line.startsWith("945e0ad4a30ec19432dfa9d5959e5d5d");
    }

    /**
     * returns specified number of parameters from start line.
     * @param start line no. to start to read. 0 is first parameter
     * except default parameter.
     * @param count number of parameters to be returned
     * @return specified number of parameters
     * @throws IOException when fails to read resource file
     */
    static TinyMT64Parameter[] getParameters(final int start, final int count)
            throws IOException {
        URL url = TinyMT32.class.getClassLoader().getResource(
                "tinymt64dc.0.65536.txt");
        ArrayList<TinyMT64Parameter> params 
            = new ArrayList<TinyMT64Parameter>();
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
                TinyMT64Parameter p = parseString(line);
                params.add(p);
                index++;
            }
        } catch (IOException e) {
            if (index > 0) {
                return params.toArray(new TinyMT64Parameter[params.size()]);
            } else {
                throw e;
            }
        } finally {
            br.close();
        }
        return params.toArray(new TinyMT64Parameter[params.size()]);
    }

    /**
     * make TinyMT64Prameter instance from line.
     * @param line line of resource file
     * @return TinyMT64Parameter instance made from line
     * @throws IOException when line does not contain
     * parameters.
     */
    private static TinyMT64Parameter parseString(final String line)
            throws IOException {
        String[] str = line.split(",");
        if (str.length < 8) {
            throw new IOException("line does not contain parameters.");
        }
        String characteristic = str[0];
        int id = Integer.parseInt(str[2]);
        int mat1 = (int) Long.parseLong(str[3], HEX_FORMAT);
        int mat2 = (int) Long.parseLong(str[4], HEX_FORMAT);
        long tmat = Long.parseLong(str[5].substring(0, 1),
                HEX_FORMAT) << (LONG_BIT_SIZE - 4);
        tmat = tmat | Long.parseLong(str[5].substring(1), HEX_FORMAT);
        int weight = Integer.parseInt(str[6], DEC_FORMAT);
        int delta = Integer.parseInt(str[7], DEC_FORMAT);
        return new TinyMT64Parameter(characteristic, id, mat1, mat2, tmat,
                weight, delta);
    }

    /**
     * return characteristic polynomial.
     * @return characteristic polynomial
     */
    F2Polynomial getCharacteristic() {
        return characteristic;
    }

    /**
     * return ID.
     * @return ID
     */
    int getId() {
        return id;
    }

    /**
     * return mat1.
     * @return mat1
     */
    long getMat1() {
        return (long) mat1 & INT_TO_LONG_MASK;
    }

    /**
     * return mat1 when x is odd number.
     * @param x number
     * @return mat1 when x is odd else 0
     */
    long getMat1(final long x) {
        if ((x & 1) == 0) {
            return 0;
        } else {
            return (long) mat1 & INT_TO_LONG_MASK;
        }
    }

    /**
     * return mat2.
     * @return mat2
     */
    long getMat2() {
        return (long) mat2 & INT_TO_LONG_MASK;
    }

    /**
     * return mat2 when x is odd number.
     * @param x integer
     * @return mat1 if x is odd else 0
     */
    long getMat2(final long x) {
        if ((x & 1) == 0) {
            return 0;
        } else {
            return (long) mat2 & INT_TO_LONG_MASK;
        }
    }

    /**
     * return tmat parameter.
     * @return tmat
     */
    long getTmat() {
        return tmat;
    }

    /**
     * return tmat if x is odd number.
     * @param x integer
     * @return return tmat if x is odd else 0
     */
    long getTmat(final long x) {
        if ((x & 1) == 0) {
            return 0;
        } else {
            return tmat;
        }
    }

    /**
     * return bit pattern depends on x is odd or not.
     * @param x integer
     * @return bit pattern depends on x is odd or not.
     */
    long getTmatDouble(final long x) {
        if ((x & 1) == 0) {
            return LONG_TO_DOUBLE_MASK;
        } else {
            return LONG_TO_DOUBLE_MASK | (tmat >>> LONG_TO_DOUBLE_SHIFT);
        }
    }

    /** return Hamming weight of characteristic polynomial.
     * @return Hamming weight of characteristic polynomial
     */
    int getWeight() {
        return weight;
    }

    /** return delta.
     * @return delta
     */
    int getDelta() {
        return delta;
    }
}
