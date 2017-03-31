
package reforest.util.mersenneTwister;

import java.math.BigInteger;

/**
 * Polynomial over the field of two elements. <b>F</b><sub>2</sub>[t]
 * 
 * This class is immutable.
 * 
 * Caution: This class is not efficient for large polynomial.
 * 
 * @author M. Saito
 */
final class F2Polynomial {
    /** internal representation of polynomial. */
    private final BigInteger pol;

    /**
     * Polynomial X<sup>1</sup> + 0.
     */
    public static final F2Polynomial X = new F2Polynomial("10");

    /**
     * constructor from string of 0 and 1.
     * 
     * @param val
     *            a string consists of 0 and 1
     */
    public F2Polynomial(final String val) {
        pol = new BigInteger(val, 2);
    }

    /**
     * constructor from string with radix.
     * 
     * @param val
     *            a string consists of numbers of radix
     * @param radix
     *            radix of the number of val
     */
    public F2Polynomial(final String val, final int radix) {
        pol = new BigInteger(val, radix);
    }

    /**
     * Constructor from BigInteger.
     * 
     * @param big
     *            BigInteger which represents polynomial
     */
    private F2Polynomial(final BigInteger big) {
        pol = big;
    }

    /**
     * If zero, this method returns -1, otherwise, returns the degree of
     * polynomial.
     * 
     * @return degree of this polynomial
     */
    public int degree() {
        return degree(pol);
    }

    /**
     * If zero, this method returns -1, otherwise, returns the degree of
     * polynomial.
     * 
     * @param pol
     *            polynomial
     * @return degree of polynomial pol
     */
    private static int degree(final BigInteger pol) {
        if (pol.equals(BigInteger.ZERO)) {
            return -1;
        } else {
            return pol.bitLength() - 1;
        }
    }

    /**
     * Addition over <b>F<sub>2</sub></b>[t].
     * 
     * @param that
     *            Polynomial
     * @return result of addition
     */
    public F2Polynomial add(final F2Polynomial that) {
        return new F2Polynomial(this.pol.xor(that.pol));
    }

    /**
     * Multiplication over <b>F<sub>2</sub></b>[t].
     * 
     * @param that
     *            Polynomial
     * @return result of multiplication
     */
    public F2Polynomial mul(final F2Polynomial that) {
        if (this.degree() >= that.degree()) {
            return new F2Polynomial(mul(this.pol, that.pol));
        } else {
            return new F2Polynomial(mul(that.pol, this.pol));
        }
    }

    /**
     * return coefficient of specified term, returned value is zero or one.
     * 
     * @param index
     *            degree of term
     * @return coefficient of specified term
     */
    public int getCoefficient(final int index) {
        if (pol.testBit(index)) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * Multiplication of BigIntegers which represents coefficient of
     * polynomials.
     * 
     * @param x
     *            polynomial
     * @param y
     *            polynomial
     * @return the result of multiplication
     */
    private static BigInteger mul(final BigInteger x, final BigInteger y) {
        BigInteger z = BigInteger.ZERO;
        BigInteger v = x;
        BigInteger w = y;
        while (!w.equals(BigInteger.ZERO)) {
            if (w.and(BigInteger.ONE).testBit(0)) {
                z = z.xor(v);
            }
            w = w.shiftRight(1);
            v = v.shiftLeft(1);
        }
        return z;
    }

    /**
     * Calculate residue of this polynomial divided by that polynomial. Using
     * means return this % that if this and that were int.
     * 
     * @param that
     *            divider
     * @return residue
     */
    public F2Polynomial mod(final F2Polynomial that) {
        return new F2Polynomial(mod(this.pol, that.pol));
    }

    /**
     * Calculate residue of polynomial y divided by that polynomial. Using means
     * return y % that if y and that were int.
     * 
     * @param y
     *            dividee
     * @param that
     *            divider
     * @return residue
     */
    private static BigInteger mod(final BigInteger y, final BigInteger that) {
        int deg = degree(that);
        int diff = degree(y) - deg;
        if (diff < 0) {
            return y;
        } else if (diff == 0) {
            return y.xor(that);
        }
        BigInteger z = y;
        BigInteger x = that.shiftLeft(diff);
        z = z.xor(x);
        int zdeg = z.bitLength() - 1;
        while (zdeg >= deg) {
            x = x.shiftRight(x.bitLength() - 1 - zdeg);
            z = z.xor(x);
            zdeg = z.bitLength() - 1;
        }
        return z;
    }

    /**
     * power of this polynomial, this<sup>pow</sup>.
     * 
     * @param pow
     *            exponent
     * @return this<sup>pow</sup>
     */
    public F2Polynomial power(final BigInteger pow) {
        return new F2Polynomial(power(this.pol, pow));
    }

    /**
     * power of polynomial x, x<sup>pow</sup>.
     * 
     * @param x
     *            polynomial
     * @param power
     *            exponent
     * @return the result of power
     */
    private BigInteger power(final BigInteger x, final BigInteger power) {
        BigInteger z = BigInteger.ONE;
        BigInteger v = x;
        BigInteger pow = power;
        while (!pow.equals(BigInteger.ZERO)) {
            if (pow.and(BigInteger.ONE).testBit(0)) {
                z = mul(z, v);
            }
            v = mul(v, v);
            pow = pow.shiftRight(1);
        }
        return z;
    }

    /**
     * returns this<sup>pow</sup> % mod.
     * 
     * @param pow
     *            exponent
     * @param mod
     *            polynomial
     * @return polynomial whose degree is less than mod polynomial
     */
    public F2Polynomial powerMod(final BigInteger pow, final F2Polynomial mod) {
        return new F2Polynomial(powerMod(this.pol, pow, mod.pol));
    }

    /**
     * returns x<sup>pow</sup> % mod.
     * 
     * @param x
     *            polynomial
     * @param power
     *            exponent
     * @param mod
     *            polynomial
     * @return polynomial whose degree is less than mod polynomial
     */
    private static BigInteger powerMod(final BigInteger x,
            final BigInteger power, final BigInteger mod) {
        BigInteger z = BigInteger.ONE;
        BigInteger s = x;
        BigInteger pow = power;
        while (!pow.equals(BigInteger.ZERO)) {
            if (pow.and(BigInteger.ONE).testBit(0)) {
                z = mul(z, s);
                z = mod(z, mod);
            }
            s = mul(s, s);
            s = mod(s, mod);
            pow = pow.shiftRight(1);
        }
        return z;
    }

    /**
     * return binary format representation of polynomial.
     * 
     * @return binary format string
     */
    @Override
    public String toString() {
        return toString(2);
    }

    /**
     * return base format representation of polynomial.
     * 
     * @param base base of format
     * @return base format string
     */
    public String toString(final int base) {
        return pol.toString(base);
    }
    /**
     * return hash code.
     * 
     * @return hash code
     */
    @Override
    public int hashCode() {
        return pol.hashCode();
    }

    /**
     * @param o
     *            object compared with this
     * @return equals or not
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof F2Polynomial) {
            F2Polynomial p = (F2Polynomial) o;
            return this.pol.equals(p.pol);
        } else {
            return false;
        }
    }
}
