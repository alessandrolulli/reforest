
package reforest.util.mersenneTwister;

import java.util.Random;

import org.apache.commons.math.random.RandomAdaptor;

/**
 * This class is an alternative implementation of 
 * java.util.concurrent.ThreadLocalRandom in
 * Java 7.
 * 
 * @author M. Saito
 * 
 */
public final class ThreadLocalRandom {
    /**
     * shift size for converting long to int.
     */
    private static final int LONG_TO_INT = 32;

    /**
     * killing default constructor.
     */
    private ThreadLocalRandom() {

    }

    /**
     * keep TinyMT instance for each thread.
     */
    private static final ThreadLocal<TinyMT32> LOCAL_RANDOM 
        = new ThreadLocal<TinyMT32>() {
        protected TinyMT32 initialValue() {
            long nanoTime = System.nanoTime();
            long threadId = Thread.currentThread().getId();
            TinyMT32 random = TinyMT32.getThreadLlocal(threadId);
            int[] seeds = new int[4];
            seeds[0] = getMSBInt(nanoTime);
            seeds[1] = (int) nanoTime;
            seeds[2] = getMSBInt(threadId);
            seeds[3] = (int) threadId;
            random.setSeed(seeds);
            return random;
        }
    };

    /**
     * get random generator for current thread.
     * 
     * @return instance of TinyMT
     */
    public static TinyMT32 currentTinyMT() {
        return LOCAL_RANDOM.get();
    }

    /**
     * get subclass of java.util.Random for current thread.
     * 
     * @return instance of subclass of java.util.Random 
     */
    public static Random current() {
        return RandomAdaptor.createAdaptor(LOCAL_RANDOM.get());
    }

    /**
     * convert long to int.
     * 
     * @param value
     *            long to be convert to int
     * @return MSBs of value
     */
    private static int getMSBInt(final long value) {
        return (int) (value >>> LONG_TO_INT);
    }
}
