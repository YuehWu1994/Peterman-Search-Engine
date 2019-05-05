package edu.uci.ics.cs221.index.inverted;


import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.List;

import javafx.util.Pair;

import java.util.Set;


/**
 * | key word | dictionary (4-byte keyword length + 12-byte 1 keyword slot) | docID list
 * |           key word   + dictionary                                      |
 */


public class SegmentInDiskManager {

    PageFileChannel pfc_dict;
    PageFileChannel pfc_posting;
    ByteBuffer dictByteBuffer; // this byte buffer is used to write keyword and dictionary
    ByteBuffer byteBuffer;
    ByteBuffer refByteBuffer; // this byte buffer is used to read keyword or docId


    public static int SLOT_SIZE = 12;


    /*
     * Define the location where we point to docID, keyword
     */
    private Location keyWordPos;
    private Location docIDPos;
    private Location dictEndPos;

    /*
     * Define the location where we point for insersion/ read
     */
    private Location pointPos;



    private enum WriteToWhere {
        To_Dictionary_File,
        To_Poisting_List
    }


    /*
     * Store position of next inserting keyword and Dictionary in dictByteBuffer
     */
    private static int nextKeywordPos;
    private static int nextDictPos;


    SegmentInDiskManager(String folder, String seg) {
        Path path_dict = Paths.get(folder + "segment_" + seg);
        Path path_poisting = Paths.get(folder + "posting_" + seg);

        pfc_dict = PageFileChannel.createOrOpen(path_dict);
        pfc_posting = PageFileChannel.createOrOpen(path_poisting);

        byteBuffer = ByteBuffer.allocate(pfc_dict.PAGE_SIZE);
        refByteBuffer = ByteBuffer.allocate(pfc_dict.PAGE_SIZE);

        keyWordPos = new Location(0, Integer.BYTES); // no end of docID, start by (0, 4)
        pointPos = new Location(0, 0);
        docIDPos = new Location(0, 0);
        dictEndPos = new Location(0, 0);

        nextKeywordPos = 0;
        nextDictPos = 0;
    }

    /**
     * ===== INSERT =====
     */

    public void insertKeyWord(String str) {
        // point
        dictByteBuffer.position(nextKeywordPos);

        insertString(str);

        // update
        nextKeywordPos += str.getBytes().length;
    }

    /*
     * | keywordPage | keyword offset | keyword length | list page | list offset | list length
     */
    public void insertMetaDataSlot(int keyLength, int valueLength)
    {
        // point
        dictByteBuffer.position(nextDictPos);

        insertInteger(keyLength, WriteToWhere.To_Dictionary_File);
        retrieveLocation(keyWordPos, keyLength, keyWordPos);

        insertShort(docIDPos.Page);
        insertShort(docIDPos.Offset);
        insertInteger(valueLength, WriteToWhere.To_Dictionary_File);
        retrieveLocation(docIDPos, valueLength, docIDPos);

        // update
        nextDictPos += SLOT_SIZE;
    }

    public void insertListOfDocID(Set<Integer> lst) {
        for (Integer i : lst) insertInteger(i, WriteToWhere.To_Poisting_List);
    }

    /**
     * ===== READ =====
     */
    public void readInitiate() {
        // set pointPos
        byteBuffer = pfc_dict.readPage(0);
        pointPos.Page = byteBuffer.getShort();
        pointPos.Offset = byteBuffer.getShort();

        // set byteBuffer to where the dictionary start (the page `pointPos` point at)
        byteBuffer = pfc_dict.readPage(pointPos.Page);
        byteBuffer.position(pointPos.Offset);

        int szKeyword = readInt(byteBuffer, pointPos, true, WriteToWhere.To_Dictionary_File);
        retrieveLocation(pointPos, szKeyword * SLOT_SIZE, dictEndPos);

        // set refByteBuffer
        refByteBuffer = pfc_dict.readPage(0);
        refByteBuffer.position(4);
    }

    public void readPostingInitiate() {
        // set pointPos
        byteBuffer = pfc_posting.readPage(0);
        pointPos.Page = 0;
        pointPos.Offset = 0;


        // set refByteBuffer
        refByteBuffer.clear();
    }


    public String readKeywordAndDict(List<Integer> dict) {
        int length = readInt(byteBuffer, pointPos, true, WriteToWhere.To_Dictionary_File);

        String keyword = readString(length, refByteBuffer, keyWordPos, false, WriteToWhere.To_Dictionary_File);

        short docPg = readShort(byteBuffer, pointPos, true, WriteToWhere.To_Dictionary_File);
        short docOffset = readShort(byteBuffer, pointPos, true, WriteToWhere.To_Dictionary_File);
        int docLength = readInt(byteBuffer, pointPos, true, WriteToWhere.To_Dictionary_File);

        dict.add((int) docPg);
        dict.add((int) docOffset);
        dict.add(docLength);

        return keyword;
    }

    public List<Integer> readDocIdList(int pageNum, int listOffset, int docIdLength) {
        List<Integer> docIdList = new ArrayList<>();
        int docSz = docIdLength / Integer.BYTES;
        Location loc = new Location(pageNum, listOffset);
        for (int i = 0; i < docSz; ++i) {
            docIdList.add(readInt(byteBuffer, loc, true, WriteToWhere.To_Poisting_List));
        }
        return docIdList;
    }

    /**
     * ===== Page Utility =====
     */

    public Pair<byte[], byte[]> splitIntegerToByte(int pivot, int i) {
        byte[] bytes = ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
        byte[] byteA = new byte[pivot];
        byte[] byteB = new byte[Integer.BYTES - pivot];

        for (int it = 0; it < pivot; ++it) byteA[it] = bytes[it];
        for (int it = pivot; it < Integer.BYTES; ++it) byteB[it - pivot] = bytes[it];

        return new Pair(byteA, byteB);
    }


    public String readString(int len, ByteBuffer bb, Location lc, boolean pointToDict, WriteToWhere writeWhere) {
        byte[] b = new byte[len];
        ByteBuffer newBb = readByte(bb, lc, pfc_dict.PAGE_SIZE - lc.Offset, len, b, writeWhere);

        // determine which byteBuffer we should assign to
        if (pointToDict) byteBuffer = newBb;
        else refByteBuffer = newBb;

        String str = new String(b);
        return str;
    }

    public short readShort(ByteBuffer bb, Location lc, boolean pointToDict, WriteToWhere writeWhere) {
        byte[] b = new byte[Short.BYTES];
        ByteBuffer newBb = readByte(bb, lc, byteBuffer.remaining(), Short.BYTES, b, writeWhere);

        // determine which byteBuffer we should assign to
        if (pointToDict) byteBuffer = newBb;
        else refByteBuffer = newBb;

        Short sh = ByteBuffer.wrap(b).getShort(); // https://stackoverflow.com/questions/7619058/convert-a-byte-array-to-integer-in-java-and-vice-versa
        return sh;
    }

    public int readInt(ByteBuffer bb, Location lc, boolean pointToDict, WriteToWhere writeWhere) {
        if(pointPos.Page != lc.Page)
        {
            if(writeWhere == WriteToWhere.To_Dictionary_File)  bb = pfc_dict.readPage(lc.Page);
            else bb = pfc_posting.readPage(lc.Page);

            pointPos.Page += 1;
            pointPos.Offset = 0;
        }
        //implement equals location
        byte[] b = new byte[Integer.BYTES];
        ByteBuffer newBb = readByte(bb, lc, byteBuffer.remaining(), Integer.BYTES, b, writeWhere);

        // determine which byteBuffer we should assign to
        if (pointToDict) byteBuffer = newBb;
        else refByteBuffer = newBb;

        int i = ByteBuffer.wrap(b).getInt();
        return i;
    }

    /**
     * @param disToEnd: distance(byte) from current pointing offset to end of page
     */
    public ByteBuffer readByte(ByteBuffer bb, Location lc, int disToEnd, int length, byte[] concat, WriteToWhere writeWhere) {

        int p = 0;
        bb.position(lc.Offset);
        for (int i = 0; i < Math.min(disToEnd, length); ++i) concat[p++] = bb.get();

        lc.Offset += Math.min(disToEnd, length);

        // if the distance is enough to read all the bytes without reading from the next page, return byte array
        if (disToEnd >= length) return bb;


        // new page
        lc.Page += 1;
        lc.Offset = 0;

        bb.clear();

        if(writeWhere == WriteToWhere.To_Dictionary_File)  bb = pfc_dict.readPage(lc.Page);
        else bb = pfc_posting.readPage(lc.Page);

        for (int i = 0; i < length - disToEnd; i++)
        {
            concat[p++] = bb.get();
        }

        // set lc offset
        lc.Offset += (length - disToEnd);

        return bb;
    }

    public void insertString(String str) {

        byte[] byteStr = str.getBytes();
        dictByteBuffer.put(byteStr);
    }

    public void insertShort(short sh) {

        dictByteBuffer.putShort(sh);
    }

    public void insertInteger(int i, WriteToWhere writeWhere) {
        if(writeWhere == WriteToWhere.To_Poisting_List){
            if (Integer.BYTES > byteBuffer.remaining()) {
                // split into 2 substring and insert into page respectively
                Pair<byte[], byte[]> byteP = splitIntegerToByte(byteBuffer.remaining(), i);

                // allocate two byte array
                allocateBytePair(byteP);
            } else {
                byteBuffer.putInt(i);
                pointPos.Offset += Integer.BYTES;
            }
        }
        else{
            dictByteBuffer.putInt(i);
        }
    }


    // allocate byte pair and update point position
    public void allocateBytePair(Pair<byte[], byte[]> byteP) {
        byteBuffer.put(byteP.getKey());

        // append page and update point position
        pfc_posting.appendPage(byteBuffer);
        pointPos.Page += 1;
        pointPos.Offset = 0;

        // insert
        byteBuffer.clear();
        byteBuffer = ByteBuffer.allocate(pfc_posting.PAGE_SIZE);

        byteBuffer.put(byteP.getValue());
        pointPos.Offset += byteP.getValue().length;
    }


    public void allocateByteBuffer(int totalLength, int map_size){
        dictByteBuffer = ByteBuffer.allocate(Short.BYTES * 2 + totalLength + Integer.BYTES + map_size * SLOT_SIZE);
    }


    // allocate the location where we start storing dictionary at the first four bytes of first page
    public void allocateKeywordStart(int totalLengthKeyword) {
        int totalLengthKeyWord = totalLengthKeyword + Integer.BYTES;

        dictByteBuffer.putShort((short) (totalLengthKeyWord / pfc_dict.PAGE_SIZE));
        dictByteBuffer.putShort((short) (totalLengthKeyWord % pfc_dict.PAGE_SIZE));

        // initialize position
        nextKeywordPos = Integer.BYTES;
        nextDictPos = totalLengthKeyWord + Integer.BYTES;

    }

    // allocate number of keyword at the location where dictionary start
    public void allocateNumberOfKeyWord(int szKeyword) {

        dictByteBuffer.position(nextKeywordPos);

        insertInteger(szKeyword, WriteToWhere.To_Dictionary_File);

    }

    public boolean hasKeyWord() {
        return !(pointPos.Page == dictEndPos.Page && pointPos.Offset == dictEndPos.Offset);
    }


    /**
     * @param loc:   original location
     * @param shift: shift by how many bytes
     * @return retrieved location
     */
    public void retrieveLocation(Location loc, int shift, Location loc2) {
        // transfer to byte-oriented distance instead of using page
        long dis = loc.Page * pfc_dict.PAGE_SIZE + loc.Offset + shift;

        loc2.Page = (short) (dis / pfc_dict.PAGE_SIZE);
        loc2.Offset = (short) (dis % pfc_dict.PAGE_SIZE);
    }

    public void appendPage() {
        pfc_posting.appendPage(byteBuffer);
    }

    public void appendAllbyte(){
        pfc_dict.appendAllBytes(dictByteBuffer);
    }

    public void close() {
        pfc_dict.close();
        pfc_posting.close();
    }

}
