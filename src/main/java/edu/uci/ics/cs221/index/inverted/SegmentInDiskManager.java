package edu.uci.ics.cs221.index.inverted;


import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.nio.ByteBuffer;

import javafx.util.Pair;


/**
 * | key word | dictionary (4-byte keyword length + 12-byte 1 keyword slot) | docID list
 * |           key word   + dictionary                                      |
 */


public class SegmentInDiskManager {

    PageFileChannel pfc_dict;
    PageFileChannel pfc_posting;
    private PageFileChannel pfc_position;
    ByteBuffer dictByteBuffer; // this byte buffer is used to write keyword and dictionary
    ByteBuffer byteBuffer; //used for writing postingList
    ByteBuffer refByteBuffer; // this byte buffer is used to read keyword or docId
    ByteBuffer positionByteBuffer; //used for writing/reading positions

    public static int SLOT_SIZE = 12;

    public static int POS_SLOT_SIZE = 10;//for positional list information 4 PAGEID | 2 SLOTNUM | 4 LENGTH

    /*
     * Define the location where we point to docID, keyword
     */
    private Location keyWordPos;
    private Location docIDPos;
    private Location dictEndPos;
    private Location posListPos;
    /*
     * Define the location where we point for insersion/ read
     */
    private Location pointPos;
    private Location posPointPos;

    private boolean positional;//to indicate if inverted index s positional


    private enum WriteToWhere {
        To_Dictionary_File,
        To_Posting_List,
        To_Position_List
    }


    /*
     * Store position of next inserting keyword and Dictionary in dictByteBuffer
     */
    private static int nextKeywordPos;
    private static int nextDictPos;


    SegmentInDiskManager(String folder, String seg, boolean positional) {
        Path path_dict = Paths.get(folder + "segment_" + seg);
        Path path_poisting = Paths.get(folder + "posting_" + seg);

        this.positional = positional;
        if(positional){
            Path path_positional = Paths.get(folder + "position_" + seg);
            pfc_position = PageFileChannel.createOrOpen(path_positional);
            positionByteBuffer = ByteBuffer.allocate(pfc_position.PAGE_SIZE);
        }
        pfc_dict = PageFileChannel.createOrOpen(path_dict);
        pfc_posting = PageFileChannel.createOrOpen(path_poisting);

        byteBuffer = ByteBuffer.allocate(pfc_dict.PAGE_SIZE);
        refByteBuffer = ByteBuffer.allocate(pfc_dict.PAGE_SIZE);

        keyWordPos = new Location(0, Integer.BYTES); // no end of docID, start by (0, 4)
        pointPos = new Location(0, 0);
        docIDPos = new Location(0, 0);
        dictEndPos = new Location(0, 0);
        posListPos = new Location(0, 0);
        posPointPos = new Location(0, 0);
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
     *         4              2            2            4
     * | keyword length | list page | list offset | list length
     */
    public void insertMetaDataSlot(int keyLength, int valueLength)
    {
        // point
        dictByteBuffer.position(nextDictPos);

        insertInteger(keyLength, WriteToWhere.To_Dictionary_File);
        retrieveLocation(keyWordPos, keyLength, keyWordPos);

        insertShort(docIDPos.Page, WriteToWhere.To_Dictionary_File);
        insertShort(docIDPos.Offset, WriteToWhere.To_Dictionary_File);
        insertInteger(valueLength, WriteToWhere.To_Dictionary_File);
        retrieveLocation(docIDPos, valueLength, docIDPos);

        // update
        nextDictPos += SLOT_SIZE;
    }

    //4 docID | 4 pgNum of posList | 2 slotNum of posList | 4 posList length
    public void insertListOfDocID(Map<Integer, List<Integer>> lst) {
            for (Map.Entry<Integer, List<Integer>> entry : lst.entrySet()){
                insertInteger(entry.getKey(), WriteToWhere.To_Posting_List);
                if (positional) {
                    List<Integer> positions = entry.getValue();
                    insertInteger((int)posListPos.Page, WriteToWhere.To_Posting_List);
                    insertShort(posListPos.Offset, WriteToWhere.To_Posting_List);
                    insertInteger(positions.size() * Integer.BYTES, WriteToWhere.To_Posting_List);
                    retrieveLocation(posListPos, positions.size() * Integer.BYTES, posListPos);
                    for(Integer pos : positions){
                        insertInteger(pos, WriteToWhere.To_Position_List);
                    }
                }
            }
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
        if(pointPos.Page != 0) {
            byteBuffer = pfc_dict.readPage(pointPos.Page);
        }
        byteBuffer.position(pointPos.Offset);

        int szKeyword = readInt(byteBuffer, pointPos, true, WriteToWhere.To_Dictionary_File);
        retrieveLocation(pointPos, szKeyword * SLOT_SIZE, dictEndPos);

        // set refByteBuffer
        if(pointPos.Page != 0) {
            refByteBuffer = pfc_dict.readPage(0);
        }
        else{
            refByteBuffer = byteBuffer.duplicate();
        }
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
    public void readPositionInitiate(){
        positionByteBuffer = pfc_position.readPage(0);
        posPointPos.Page = 0;
        posPointPos.Offset = 0;
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

    public Map<Integer, List<Integer>> readDocIdList(int pageNum, int listOffset, int docIdLength) {
        Map<Integer, List<Integer>> docIdList = new TreeMap<>();
        int docSz = docIdLength;
        if(positional) {
             docSz = docSz / (Integer.BYTES + POS_SLOT_SIZE);
        }
        else{
            docSz = docSz / Integer.BYTES;
        }
        Location loc = new Location(pageNum, listOffset);
        for (int i = 0; i < docSz; ++i) {
            List<Integer> positionListMetaData = new ArrayList<>();
            int docId = readInt(byteBuffer, loc, true, WriteToWhere.To_Posting_List);
            if(positional) {
                positionListMetaData.add(readInt(byteBuffer, loc, true, WriteToWhere.To_Posting_List));
                positionListMetaData.add((int) readShort(byteBuffer, loc, true, WriteToWhere.To_Posting_List));
                positionListMetaData.add(readInt(byteBuffer, loc, true, WriteToWhere.To_Posting_List));
            }
            docIdList.put(docId, positionListMetaData);
        }
        return docIdList;
    }
    public List<Integer> readPosList(int pageNum, int listOffset, int posListSize) {
        List<Integer> posList = new ArrayList<>();
        int posSz = posListSize / Integer.BYTES;
        Location loc = new Location(pageNum, listOffset);
        for (int i = 0; i < posSz; ++i) {
            posList.add(readInt(positionByteBuffer, loc, true, WriteToWhere.To_Position_List));
        }
        return posList;
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
    public Pair<byte[], byte[]> splitShortToByte(int pivot, short sh) {
        byte[] bytes = ByteBuffer.allocate(Integer.BYTES).putInt(sh).array();
        byte[] byteA = new byte[pivot];
        byte[] byteB = new byte[Integer.BYTES - pivot];

        for (int it = 0; it < pivot; ++it) byteA[it] = bytes[it];
        for (int it = pivot; it < Short.BYTES; ++it) byteB[it - pivot] = bytes[it];

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
        if(writeWhere == WriteToWhere.To_Position_List){
            ByteBuffer newBb = readByte(bb, lc, positionByteBuffer.remaining(), Short.BYTES, b, writeWhere);
            positionByteBuffer = newBb;
        }
        else {
            ByteBuffer newBb = readByte(bb, lc, byteBuffer.remaining(), Short.BYTES, b, writeWhere);
            // determine which byteBuffer we should assign to
            if (pointToDict) byteBuffer = newBb;
            else refByteBuffer = newBb;
        }
        Short sh = ByteBuffer.wrap(b).getShort(); // https://stackoverflow.com/questions/7619058/convert-a-byte-array-to-integer-in-java-and-vice-versa
        return sh;
    }

    public int readInt(ByteBuffer bb, Location lc, boolean pointToDict, WriteToWhere writeWhere) {
        if (writeWhere == WriteToWhere.To_Position_List) {
            if (posPointPos.Page != lc.Page) {
                    bb = pfc_position.readPage(lc.Page);

                posPointPos.Page += 1;
                posPointPos.Offset = 0;
            }
            //implement equals location
            byte[] b = new byte[Integer.BYTES];
            ByteBuffer newBb = readByte(bb, lc, positionByteBuffer.remaining(), Integer.BYTES, b, writeWhere);

            positionByteBuffer = newBb;
            int i = ByteBuffer.wrap(b).getInt();
            return i;
        } else {
            if (pointPos.Page != lc.Page) {
                if (writeWhere == WriteToWhere.To_Dictionary_File) {
                    bb = pfc_dict.readPage(lc.Page);
                } else {
                    bb = pfc_posting.readPage(lc.Page);
                }
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

        if(writeWhere == WriteToWhere.To_Dictionary_File){
            bb = pfc_dict.readPage(lc.Page);
        }
        else if(writeWhere == WriteToWhere.To_Posting_List) {
            bb = pfc_posting.readPage(lc.Page);
        }
        else {
            bb = pfc_position.readPage(lc.Page);
        }

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

    public void insertShort(short sh, WriteToWhere writeWhere) {
        if(writeWhere == WriteToWhere.To_Posting_List){
            if (Short.BYTES > byteBuffer.remaining()) {
                // split into 2 substring and insert into page respectively
                Pair<byte[], byte[]> byteP = splitShortToByte(byteBuffer.remaining(), sh);

                // allocate two byte array
                allocateBytePair(byteP, writeWhere);
            } else {
                byteBuffer.putShort(sh);
                pointPos.Offset += Short.BYTES;
            }
        }
        else {
            dictByteBuffer.putShort(sh);
        }
    }

    public void insertInteger(int i, WriteToWhere writeWhere) {
        if(writeWhere == WriteToWhere.To_Posting_List){
            if (Integer.BYTES > byteBuffer.remaining()) {
                // split into 2 substring and insert into page respectively
                Pair<byte[], byte[]> byteP = splitIntegerToByte(byteBuffer.remaining(), i);

                // allocate two byte array
                allocateBytePair(byteP, writeWhere);
            } else {
                byteBuffer.putInt(i);
                pointPos.Offset += Integer.BYTES;
            }
        }
        else if(writeWhere == WriteToWhere.To_Position_List){
            if (Integer.BYTES > positionByteBuffer.remaining()) {
                // split into 2 substring and insert into page respectively
                Pair<byte[], byte[]> byteP = splitIntegerToByte(positionByteBuffer.remaining(), i);

                // allocate two byte array
                allocateBytePair(byteP, writeWhere);
            } else {
                positionByteBuffer.putInt(i);
                posPointPos.Offset += Integer.BYTES;
            }
        }
        else{
            dictByteBuffer.putInt(i);
        }
    }


    // allocate byte pair and update point position
    public void allocateBytePair(Pair<byte[], byte[]> byteP, WriteToWhere writeToWhere) {
        if(writeToWhere == WriteToWhere.To_Posting_List) {
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
        else{
            positionByteBuffer.put(byteP.getKey());

            // append page and update point position
            pfc_position.appendPage(positionByteBuffer);
            posPointPos.Page += 1;
            posPointPos.Offset = 0;

            // insert
            positionByteBuffer.clear();
            positionByteBuffer = ByteBuffer.allocate(pfc_position.PAGE_SIZE);

            positionByteBuffer.put(byteP.getValue());
            posPointPos.Offset += byteP.getValue().length;
        }
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
        if(positional){
            pfc_position.appendPage(positionByteBuffer);
        }
    }

    public void appendAllbyte(){
        pfc_dict.appendAllBytes(dictByteBuffer);
    }

    public void close() {
        pfc_dict.close();
        pfc_posting.close();
        if(positional){
            pfc_position.close();
        }
    }

}
