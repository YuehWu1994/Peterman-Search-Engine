
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package edu.uci.ics.cs221.analysis;
import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.StringTokenizer;

/**
 * Methods for manipulating arrays.
 *
 * @lucene.internal
 */

public final class ArrayUtil {

    /** Maximum length for an array (Integer.MAX_VALUE - RamUsageEstimator.NUM_BYTES_ARRAY_HEADER). */
    public static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 20;

    private ArrayUtil() {} // no instance

  /*
     Begin Apache Harmony code
     Revision taken on Friday, June 12. https://svn.apache.org/repos/asf/harmony/enhanced/classlib/archive/java6/modules/luni/src/main/java/java/lang/Integer.java
   */

    /**
     * Parses a char array into an int.
     * @param chars the character array
     * @param offset The offset into the array
     * @param len The length
     * @return the int
     * @throws NumberFormatException if it can't parse
     */


  /*
 END APACHE HARMONY CODE
  */

    /** Returns an array size &gt;= minTargetSize, generally
     *  over-allocating exponentially to achieve amortized
     *  linear-time cost as the array grows.
     *
     *  NOTE: this was originally borrowed from Python 2.4.2
     *  listobject.c sources (attribution in LICENSE.txt), but
     *  has now been substantially changed based on
     *  discussions from java-dev thread with subject "Dynamic
     *  array reallocation algorithms", started on Jan 12
     *  2010.
     *
     * @param minTargetSize Minimum required value to be returned.
     * @param bytesPerElement Bytes used by each element of
     *
     * @lucene.internal
     */

    public static final String JVM_SPEC_VERSION = System.getProperty("java.specification.version");

    public static final boolean JRE_IS_64BIT;
    private static final int JVM_MAJOR_VERSION;
    private static final int JVM_MINOR_VERSION;
    public static final String OS_ARCH = System.getProperty("os.arch");

    static {
        final StringTokenizer st = new StringTokenizer(JVM_SPEC_VERSION, ".");
        JVM_MAJOR_VERSION = Integer.parseInt(st.nextToken());
        if (st.hasMoreTokens()) {
            JVM_MINOR_VERSION = Integer.parseInt(st.nextToken());
        } else {
            JVM_MINOR_VERSION = 0;
        }
        boolean is64Bit = false;
        String datamodel = null;
        try {
            datamodel = System.getProperty("sun.arch.data.model");
            if (datamodel != null) {
                is64Bit = datamodel.contains("64");
            }
        } catch (SecurityException ex) {}
        if (datamodel == null) {
            if (OS_ARCH != null && OS_ARCH.contains("64")) {
                is64Bit = true;
            } else {
                is64Bit = false;
            }
        }
        JRE_IS_64BIT = is64Bit;
    }

    public static int oversize(int minTargetSize, int bytesPerElement) {

        if (minTargetSize < 0) {
            // catch usage that accidentally overflows int
            throw new IllegalArgumentException("invalid array size " + minTargetSize);
        }

        if (minTargetSize == 0) {
            // wait until at least one element is requested
            return 0;
        }

        if (minTargetSize > MAX_ARRAY_LENGTH) {
            throw new IllegalArgumentException("requested array size " + minTargetSize + " exceeds maximum array in java (" + MAX_ARRAY_LENGTH + ")");
        }

        // asymptotic exponential growth by 1/8th, favors
        // spending a bit more CPU to not tie up too much wasted
        // RAM:
        int extra = minTargetSize >> 3;

        if (extra < 3) {
            // for very small arrays, where constant overhead of
            // realloc is presumably relatively high, we grow
            // faster
            extra = 3;
        }

        int newSize = minTargetSize + extra;

        // add 7 to allow for worst case byte alignment addition below:
        if (newSize+7 < 0 || newSize+7 > MAX_ARRAY_LENGTH) {
            // int overflowed, or we exceeded the maximum array length
            return MAX_ARRAY_LENGTH;
        }

        if (JRE_IS_64BIT) {
            // round up to 8 byte alignment in 64bit env
            switch(bytesPerElement) {
                case 4:
                    // round up to multiple of 2
                    return (newSize + 1) & 0x7ffffffe;
                case 2:
                    // round up to multiple of 4
                    return (newSize + 3) & 0x7ffffffc;
                case 1:
                    // round up to multiple of 8
                    return (newSize + 7) & 0x7ffffff8;
                case 8:
                    // no rounding
                default:
                    // odd (invalid?) size
                    return newSize;
            }
        } else {
            // round up to 4 byte alignment in 64bit env
            switch(bytesPerElement) {
                case 2:
                    // round up to multiple of 2
                    return (newSize + 1) & 0x7ffffffe;
                case 1:
                    // round up to multiple of 4
                    return (newSize + 3) & 0x7ffffffc;
                case 4:
                case 8:
                    // no rounding
                default:
                    // odd (invalid?) size
                    return newSize;
            }
        }
    }

    /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
    public static <T> T[] growExact(T[] array, int newLength) {
        Class<? extends Object[]> type = array.getClass();
        @SuppressWarnings("unchecked")
        T[] copy = (type == Object[].class)
                ? (T[]) new Object[newLength]
                : (T[]) Array.newInstance(type.getComponentType(), newLength);
        System.arraycopy(array, 0, copy, 0, array.length);
        return copy;
    }

    /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
    public static <T> T[] grow(T[] array, int minSize) {
        assert minSize >= 0 : "size must be positive (got " + minSize + "): likely integer overflow?";
        if (array.length < minSize) {
            final int newLength = oversize(minSize, 8);
            return growExact(array, newLength);
        } else
            return array;
    }

    /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
    public static short[] growExact(short[] array, int newLength) {
        short[] copy = new short[newLength];
        System.arraycopy(array, 0, copy, 0, array.length);
        return copy;
    }

    /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
    public static short[] grow(short[] array, int minSize) {
        assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
        if (array.length < minSize) {
            return growExact(array, oversize(minSize, Short.BYTES));
        } else
            return array;
    }

    /** Returns a larger array, generally over-allocating exponentially */
    public static short[] grow(short[] array) {
        return grow(array, 1 + array.length);
    }

    /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
    public static float[] growExact(float[] array, int newLength) {
        float[] copy = new float[newLength];
        System.arraycopy(array, 0, copy, 0, array.length);
        return copy;
    }

    /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
    public static float[] grow(float[] array, int minSize) {
        assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
        if (array.length < minSize) {
            float[] copy = new float[oversize(minSize, Float.BYTES)];
            System.arraycopy(array, 0, copy, 0, array.length);
            return copy;
        } else
            return array;
    }

    /** Returns a larger array, generally over-allocating exponentially */
    public static float[] grow(float[] array) {
        return grow(array, 1 + array.length);
    }

    /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
    public static double[] growExact(double[] array, int newLength) {
        double[] copy = new double[newLength];
        System.arraycopy(array, 0, copy, 0, array.length);
        return copy;
    }

    /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
    public static double[] grow(double[] array, int minSize) {
        assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
        if (array.length < minSize) {
            return growExact(array, oversize(minSize, Double.BYTES));
        } else
            return array;
    }

    /** Returns a larger array, generally over-allocating exponentially */
    public static double[] grow(double[] array) {
        return grow(array, 1 + array.length);
    }

    /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
    public static int[] growExact(int[] array, int newLength) {
        int[] copy = new int[newLength];
        System.arraycopy(array, 0, copy, 0, array.length);
        return copy;
    }

    /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
    public static int[] grow(int[] array, int minSize) {
        assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
        if (array.length < minSize) {
            return growExact(array, oversize(minSize, Integer.BYTES));
        } else
            return array;
    }

    /** Returns a larger array, generally over-allocating exponentially */
    public static int[] grow(int[] array) {
        return grow(array, 1 + array.length);
    }

    /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
    public static long[] growExact(long[] array, int newLength) {
        long[] copy = new long[newLength];
        System.arraycopy(array, 0, copy, 0, array.length);
        return copy;
    }

    /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
    public static long[] grow(long[] array, int minSize) {
        assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
        if (array.length < minSize) {
            return growExact(array, oversize(minSize, Long.BYTES));
        } else
            return array;
    }

    /** Returns a larger array, generally over-allocating exponentially */
    public static long[] grow(long[] array) {
        return grow(array, 1 + array.length);
    }

    /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
    public static byte[] growExact(byte[] array, int newLength) {
        byte[] copy = new byte[newLength];
        System.arraycopy(array, 0, copy, 0, array.length);
        return copy;
    }

    /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
    public static byte[] grow(byte[] array, int minSize) {
        assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
        if (array.length < minSize) {
            return growExact(array, oversize(minSize, Byte.BYTES));
        } else
            return array;
    }

    /** Returns a larger array, generally over-allocating exponentially */
    public static byte[] grow(byte[] array) {
        return grow(array, 1 + array.length);
    }

    /** Returns a new array whose size is exact the specified {@code newLength} without over-allocating */
    public static char[] growExact(char[] array, int newLength) {
        char[] copy = new char[newLength];
        System.arraycopy(array, 0, copy, 0, array.length);
        return copy;
    }

    /** Returns an array whose size is at least {@code minSize}, generally over-allocating exponentially */
    public static char[] grow(char[] array, int minSize) {
        assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
        if (array.length < minSize) {
            return growExact(array, oversize(minSize, Character.BYTES));
        } else
            return array;
    }

    /** Returns a larger array, generally over-allocating exponentially */
    public static char[] grow(char[] array) {
        return grow(array, 1 + array.length);
    }

    /**
     * Returns hash of chars in range start (inclusive) to
     * end (inclusive)
     */
    public static int hashCode(char[] array, int start, int end) {
        int code = 0;
        for (int i = end - 1; i >= start; i--)
            code = code * 31 + array[i];
        return code;
    }

    /** Swap values stored in slots <code>i</code> and <code>j</code> */
    public static <T> void swap(T[] arr, int i, int j) {
        final T tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }



    /** Reorganize {@code arr[from:to[} so that the element at offset k is at the
     *  same position as if {@code arr[from:to[} was sorted, and all elements on
     *  its left are less than or equal to it, and all elements on its right are
     *  greater than or equal to it.
     *  This runs in linear time on average and in {@code n log(n)} time in the
     *  worst case.*/

    /**
     * Copies the specified range of the given array into a new sub array.
     * @param array the input array
     * @param from  the initial index of range to be copied (inclusive)
     * @param to    the final index of range to be copied (exclusive)
     */
    public static byte[] copyOfSubArray(byte[] array, int from, int to) {
        final byte[] copy = new byte[to-from];
        System.arraycopy(array, from, copy, 0, to-from);
        return copy;
    }

    /**
     * Copies the specified range of the given array into a new sub array.
     * @param array the input array
     * @param from  the initial index of range to be copied (inclusive)
     * @param to    the final index of range to be copied (exclusive)
     */
    public static char[] copyOfSubArray(char[] array, int from, int to) {
        final char[] copy = new char[to-from];
        System.arraycopy(array, from, copy, 0, to-from);
        return copy;
    }

    /**
     * Copies the specified range of the given array into a new sub array.
     * @param array the input array
     * @param from  the initial index of range to be copied (inclusive)
     * @param to    the final index of range to be copied (exclusive)
     */
    public static short[] copyOfSubArray(short[] array, int from, int to) {
        final short[] copy = new short[to-from];
        System.arraycopy(array, from, copy, 0, to-from);
        return copy;
    }

    /**
     * Copies the specified range of the given array into a new sub array.
     * @param array the input array
     * @param from  the initial index of range to be copied (inclusive)
     * @param to    the final index of range to be copied (exclusive)
     */
    public static int[] copyOfSubArray(int[] array, int from, int to) {
        final int[] copy = new int[to-from];
        System.arraycopy(array, from, copy, 0, to-from);
        return copy;
    }

    /**
     * Copies the specified range of the given array into a new sub array.
     * @param array the input array
     * @param from  the initial index of range to be copied (inclusive)
     * @param to    the final index of range to be copied (exclusive)
     */
    public static long[] copyOfSubArray(long[] array, int from, int to) {
        final long[] copy = new long[to-from];
        System.arraycopy(array, from, copy, 0, to-from);
        return copy;
    }

    /**
     * Copies the specified range of the given array into a new sub array.
     * @param array the input array
     * @param from  the initial index of range to be copied (inclusive)
     * @param to    the final index of range to be copied (exclusive)
     */
    public static float[] copyOfSubArray(float[] array, int from, int to) {
        final float[] copy = new float[to-from];
        System.arraycopy(array, from, copy, 0, to-from);
        return copy;
    }

    /**
     * Copies the specified range of the given array into a new sub array.
     * @param array the input array
     * @param from  the initial index of range to be copied (inclusive)
     * @param to    the final index of range to be copied (exclusive)
     */
    public static double[] copyOfSubArray(double[] array, int from, int to) {
        final double[] copy = new double[to-from];
        System.arraycopy(array, from, copy, 0, to-from);
        return copy;
    }

    /**
     * Copies the specified range of the given array into a new sub array.
     * @param array the input array
     * @param from  the initial index of range to be copied (inclusive)
     * @param to    the final index of range to be copied (exclusive)
     */
    public static <T> T[] copyOfSubArray(T[] array, int from, int to) {
        final int subLength = to - from;
        final Class<? extends Object[]> type = array.getClass();
        @SuppressWarnings("unchecked")
        final T[] copy = (type == Object[].class)
                ? (T[]) new Object[subLength]
                : (T[]) Array.newInstance(type.getComponentType(), subLength);
        System.arraycopy(array, from, copy, 0, subLength);
        return copy;
    }
}