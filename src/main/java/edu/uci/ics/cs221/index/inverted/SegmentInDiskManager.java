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

    public static int SLOT_SIZE = 16;


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


    SegmentInDiskManager(String folder, String seg) {
        Path path_dict = Paths.get(folder + "segment_" + seg);
        Path path_poisting = Paths.get(folder + "posting_" + seg);
        System.out.println(path_dict.toString());

        pfc_dict = PageFileChannel.createOrOpen(path_dict);
        pfc_posting = PageFileChannel.createOrOpen(path_poisting);

        byteBuffer = ByteBuffer.allocate(pfc_dict.PAGE_SIZE);
        refByteBuffer = ByteBuffer.allocate(pfc_dict.PAGE_SIZE);

        keyWordPos = new Location(0, Integer.BYTES); // no end of docID, start by (0, 4)
        pointPos = new Location(0, 0);
        docIDPos = new Location(0, 0);
        dictEndPos = new Location(0, 0);
    }

    /**
     * ===== INSERT =====
     */

    public void insertKeyWord(String str) {
        insertString(str);
        assert (byteBuffer.position() == pointPos.Offset) : "pointer " + pointPos.Offset + " and buffer position " + byteBuffer.position() + " not match";
    }

    /*
     * | keywordPage | keyword offset | keyword length | list page | list offset | list length
     */
    public void insertMetaDataSlot(int keyLength, int valueLength)
    {
        insertShort(keyWordPos.Page);
        insertShort(keyWordPos.Offset);
        insertInteger(keyLength, WriteToWhere.To_Dictionary_File);
        retrieveLocation(keyWordPos, keyLength, keyWordPos);

        insertShort(docIDPos.Page);
        insertShort(docIDPos.Offset);
        insertInteger(valueLength, WriteToWhere.To_Dictionary_File);
        retrieveLocation(docIDPos, valueLength, docIDPos);

    }

    public void insertListOfDocID(Set<Integer> lst) {
        for (Integer i : lst) insertInteger(i, WriteToWhere.To_Poisting_List);
        //assert (byteBuffer.position() == pointPos.Offset) : "pointer " + pointPos.Offset + " and buffer position " + byteBuffer.position() + " not match";
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

        // set docIDPos
        int szKeyword = readInt(byteBuffer, pointPos, true, WriteToWhere.To_Dictionary_File);
        retrieveLocation(pointPos, szKeyword * SLOT_SIZE, dictEndPos);
        //System.out.println("doc ID offset is: " + docIDPos.Offset);

        // set refByteBuffer
        refByteBuffer = pfc_dict.readPage(0);
        refByteBuffer.position(4);
    }

    public void readPostingInitiate() {
        // set pointPos
        byteBuffer = pfc_posting.readPage(0);
        pointPos.Page = 0;
        pointPos.Offset = 0;


        // set docIDPos


        // set refByteBuffer
        refByteBuffer.clear();
    }


    public String readKeywordAndDict(List<Integer> dict) {
        short pg = readShort(byteBuffer, pointPos, true, WriteToWhere.To_Dictionary_File);
        short offset = readShort(byteBuffer, pointPos, true, WriteToWhere.To_Dictionary_File);
        int length = readInt(byteBuffer, pointPos, true, WriteToWhere.To_Dictionary_File);

        String keyword = readString(length, refByteBuffer, keyWordPos, false, WriteToWhere.To_Dictionary_File);

        short docPg = readShort(byteBuffer, pointPos, true, WriteToWhere.To_Dictionary_File);
        short docOffset = readShort(byteBuffer, pointPos, true, WriteToWhere.To_Dictionary_File);
        int docLength = readInt(byteBuffer, pointPos, true, WriteToWhere.To_Dictionary_File);

        //System.out.println("\n");

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
    public Pair<byte[], byte[]> splitShortToByte(int pivot, short sh) {
        byte[] bytes = ByteBuffer.allocate(Integer.BYTES).putInt(sh).array();
        byte[] byteA = new byte[pivot];
        byte[] byteB = new byte[Integer.BYTES - pivot];

        for (int it = 0; it < pivot; ++it) byteA[it] = bytes[it];
        for (int it = pivot; it < Short.BYTES; ++it) byteB[it - pivot] = bytes[it];

        return new Pair(byteA, byteB);
    }

    public Pair<byte[], byte[]> splitIntegerToByte(int pivot, int i) {
        byte[] bytes = ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
        byte[] byteA = new byte[pivot];
        byte[] byteB = new byte[Integer.BYTES - pivot];

        for (int it = 0; it < pivot; ++it) byteA[it] = bytes[it];
        for (int it = pivot; it < Integer.BYTES; ++it) byteB[it - pivot] = bytes[it];

        return new Pair(byteA, byteB);
    }

    public Pair<byte[], byte[]> splitStringToByte(int pivot, String str) {

        byte[] byteStr = str.getBytes();
        byte[] byteA = new byte[pivot];
        byte[] byteB = new byte[byteStr.length - pivot];

        for (int i = 0; i < pivot; ++i) byteA[i] = byteStr[i];
        for (int i = 0; i < byteStr.length - pivot; ++i) byteB[i] = byteStr[i + pivot];

        return new Pair(byteA, byteB);
    }

    public String readString(int len, ByteBuffer bb, Location lc, boolean pointToDict, WriteToWhere writeWhere) {
        //System.out.print("Read string at (" + lc.Page + "," + lc.Offset + "), length is: " + len);
        byte[] b = new byte[len];
        ByteBuffer newBb = readByte(bb, lc, pfc_dict.PAGE_SIZE - lc.Offset, len, b, writeWhere);

        // determine which byteBuffer we should assign to
        if (pointToDict) byteBuffer = newBb;
        else refByteBuffer = newBb;

        String str = new String(b);
        //System.out.print(", value is: " + str + '\n');
        return str;
    }

    public short readShort(ByteBuffer bb, Location lc, boolean pointToDict, WriteToWhere writeWhere) {
        //System.out.print("Read short at (" + lc.Page + "," + lc.Offset + "), length is: 2");
        byte[] b = new byte[Short.BYTES];
        ByteBuffer newBb = readByte(bb, lc, byteBuffer.remaining(), Short.BYTES, b, writeWhere);

        // determine which byteBuffer we should assign to
        if (pointToDict) byteBuffer = newBb;
        else refByteBuffer = newBb;

        Short sh = ByteBuffer.wrap(b).getShort(); // https://stackoverflow.com/questions/7619058/convert-a-byte-array-to-integer-in-java-and-vice-versa
        //System.out.print(", value is: " + sh + '\n');
        return sh;
    }

    public int readInt(ByteBuffer bb, Location lc, boolean pointToDict, WriteToWhere writeWhere) {
        if(pointPos.Page != lc.Page)
        {
            if(writeWhere == WriteToWhere.To_Dictionary_File)  bb = pfc_dict.readPage(lc.Page);
            else bb = pfc_posting.readPage(lc.Page);
        }
        //implement equals location
        //System.out.print("Read integer at (" + lc.Page + "," + lc.Offset + "), length is: 4");
        byte[] b = new byte[Integer.BYTES];
        ByteBuffer newBb = readByte(bb, lc, byteBuffer.remaining(), Integer.BYTES, b, writeWhere);

        // determine which byteBuffer we should assign to
        if (pointToDict) byteBuffer = newBb;
        else refByteBuffer = newBb;

        int i = ByteBuffer.wrap(b).getInt();
        //System.out.print(", value is: " + i + '\n');
        return i;
    }

    /**
     * @param disToEnd: distance(byte) from current pointing offset to end of page
     */
    public ByteBuffer readByte(ByteBuffer bb, Location lc, int disToEnd, int length, byte[] concat, WriteToWhere writeWhere) {
        //byte [] concat = new byte[length];

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
        //System.out.println("Insert string " + str + " at (" + pointPos.Page + "," + pointPos.Offset + "), length is: " + str.getBytes().length);
//        if (str.getBytes().length > byteBuffer.remaining()) {
//            // split into 2 substring and insert into page respectively
//            Pair<byte[], byte[]> byteP = splitStringToByte(byteBuffer.remaining(), str);
//
//            // allocate two byte array
//            allocateBytePair(byteP);
//        } else {
//            byte[] byteStr = str.getBytes();
//            byteBuffer.put(byteStr);
//            pointPos.Offset += byteStr.length;
//        }
        byte[] byteStr = str.getBytes();
        dictByteBuffer.put(byteStr);
    }

    public void insertShort(short sh) {
        //System.out.println("Insert short " + sh + " at (" + pointPos.Page + "," + pointPos.Offset + "), length is: 2");
//        if (Short.BYTES > byteBuffer.remaining()) {
//            // split into 2 substring and insert into page respectively
//            Pair<byte[], byte[]> byteP = splitShortToByte(byteBuffer.remaining(), sh);
//
//            // allocate two byte array
//            allocateBytePair(byteP);
//        } else {
//            byteBuffer.putShort(sh);
//            pointPos.Offset += Short.BYTES;
//        }

        dictByteBuffer.putShort(sh);
    }

    public void insertInteger(int i, WriteToWhere writeWhere) {
        //System.out.println("Insert integer " + i + " at (" + pointPos.Page + "," + pointPos.Offset + "), length is: 4");
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


        //pointPos.Offset += Integer.BYTES;  ####
        //System.out.println("====== Keyword initial position is: " + pointPos.Page + ' ' + pointPos.Offset + " ====== ");
    }

    // allocate number of keyword at the location where dictionary start
    public void allocateNumberOfKeyWord(int szKeyword) {
        //System.out.println("====== Dictionary initial position is: " + pointPos.Page + ' ' + pointPos.Offset + " ====== ");

        insertInteger(szKeyword, WriteToWhere.To_Dictionary_File);

        // assign docID Position

        //retrieveLocation(pointPos,  SLOT_SIZE * szKeyword, docIDPos); ####

        //System.out.println("====== Doc Id initial position is: " + docIDPos.Page + ' ' + docIDPos.Offset + " ====== ");

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
