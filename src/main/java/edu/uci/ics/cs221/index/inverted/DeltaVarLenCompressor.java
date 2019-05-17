package edu.uci.ics.cs221.index.inverted;


import java.util.ArrayList;
import java.util.List;
import java.io.*;
import java.util.BitSet;

/**
 * Implement this compressor with Delta Encoding and Variable-Length Encoding.
 * See Project 3 description for details.
 */
public class DeltaVarLenCompressor implements Compressor {

    @Override
    public byte[] encode(List<Integer> integers) {

        //throw new UnsupportedOperationException();

        int n = integers.size(), prev = 0;
        ByteArrayOutputStream res = new ByteArrayOutputStream();

        for(int i = 0; i < n; ++i){
            byte [] b = encodeInteger(prev, integers.get(i));

            // write byte array to ByteArrayOutputStream, and handle exception
            try{
                res.write(b);
            } catch (IOException e){
                e.printStackTrace();
            }

            prev = integers.get(i);
        }


        return res.toByteArray();
    }

    @Override
    public List<Integer> decode(byte[] bytes, int start, int length) {

        //throw new UnsupportedOperationException();

        List<Integer> res =  new ArrayList<>();
        int startValue = 0, offset = 0; // use startValue and offset to extract original integer

        int n = bytes.length;
        for(int i = start; (i < (start + length) && i < n); ++i){
            byte b = bytes[i];
            if(((b >> 7) & 1) != 0){
                offset = (offset << 7) + ((bytes[i] & 0xFF) - 128);
            }
            else{
                offset = (offset << 7) + (bytes[i] & 0xFF);
                startValue += offset;
                res.add(startValue);
                offset = 0;
            }
        }

        return res;
    }


    public byte[] encodeInteger(int prev, int cur){
        int num = cur - prev;


        // implement delta encoding and variable-length encoding
        BitSet bits = new BitSet();
        int index = 0;


        while(num > 0){
            if(index % 8 != 7){
                bits.set(index, (((num%2) == 1) ? true : false));
                num = (num >>> 1);
            }
            else bits.set(index, false);
            ++index;
        }

        while(index % 8 != 0) bits.set(index++, false);


        // set variable bit
        for(int i = 15; i < index; i += 8) bits.set(i, true);


        // transform bitset to byte array
        byte [] b = (bits.isEmpty() ? new byte[1] : bits.toByteArray());

        // reverse byte array
        int left = 0, right = b.length-1;
        while(left < right){
            byte tmp = b[left];
            b[left++] = b[right];
            b[right--] = tmp;
        }

        return b; // not sure
    }




    // original integer to -> difference
    // integer(difference) to bytes array
        // represent integer in binary    https://stackoverflow.com/questions/2473597/bitset-to-and-from-integer-long
        // delta encoding and variable-length encoding   https://stackoverflow.com/questions/2473597/bitset-to-and-from-integer-long
        // store in bytes array  https://www.programcreek.com/java-api-examples/?class=java.util.BitSet&method=toByteArray




    // bytes array to integer(difference)
        // extract range 1, 1, .... 0  https://stackoverflow.com/questions/9354860/how-to-get-the-value-of-a-bit-at-a-certain-position-from-a-byte
                                        // byte <-> integer https://way2java.com/casting-operations/java-byte-to-int/
        // concat bits (might not use)
        // transform to integer
    // original integer + difference



}
