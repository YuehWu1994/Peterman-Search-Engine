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
    private PageFileChannel pfc_posMeta;
    ByteBuffer dictByteBuffer; // this byte buffer is used to write keyword and dictionary
    ByteBuffer byteBuffer; //used for writing postingList
    ByteBuffer refByteBuffer; // this byte buffer is used to read keyword or docId
    ByteBuffer positionByteBuffer; //used for writing/reading positions
    ByteBuffer posMetaByteBuffer;

    public static int SLOT_SIZE = 18;//added int for also storing number of doc ids per posting list for search
    private static int POSITION_SLOT_SIZE = 10;
    private int docIdCount;
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
    private Location metaPos;

    private Compressor compressor;//to indicate if inverted index s positional


    private enum WriteToWhere {
        To_Dictionary_File,
        To_Posting_List,
        To_Position_List,
        To_Pos_Meta_File
    }


    /*
     * Store position of next inserting keyword and Dictionary in dictByteBuffer
     */
    private static int nextKeywordPos;
    private static int nextDictPos;


    SegmentInDiskManager(String folder, String seg, Compressor compressor) {
        Path path_dict = Paths.get(folder + "segment_" + seg);
        Path path_poisting = Paths.get(folder + "posting_" + seg);

        this.compressor = compressor;
        docIdCount = 0;
        if (isPositional()) {
            Path path_positional = Paths.get(folder + "position_" + seg);
            pfc_position = PageFileChannel.createOrOpen(path_positional);
            positionByteBuffer = ByteBuffer.allocate(pfc_position.PAGE_SIZE);
            Path path_posMeta = Paths.get(folder + "meta_" + seg);
            pfc_posMeta = PageFileChannel.createOrOpen(path_posMeta);
            posMetaByteBuffer = ByteBuffer.allocate(pfc_posMeta.PAGE_SIZE);
        }
        pfc_dict = PageFileChannel.createOrOpen(path_dict);
        pfc_posting = PageFileChannel.createOrOpen(path_poisting);

        byteBuffer = ByteBuffer.allocate(pfc_dict.PAGE_SIZE);
        refByteBuffer = ByteBuffer.allocate(pfc_dict.PAGE_SIZE);

        keyWordPos = new Location(0, Integer.BYTES + Short.BYTES); // no end of docID, start by (0, 4)
        pointPos = new Location(0, 0);
        docIDPos = new Location(0, 0);
        dictEndPos = new Location(0, 0);
        posListPos = new Location(0, 0);
        posPointPos = new Location(0, 0);
        metaPos = new Location(0, 0);
        nextKeywordPos = 0;
        nextDictPos = 0;
    }

    /**
     * ===== ALLOCATION =====
     */
    public void allocateByteBuffer(int totalLength, int map_size) {
        dictByteBuffer = ByteBuffer.allocate(Integer.BYTES + Short.BYTES + totalLength + Integer.BYTES + map_size * SLOT_SIZE);
    }


    // allocate the location where we start storing dictionary at the first four bytes of first page
    public void allocateKeywordStart(int totalLengthKeyword) {
        int totalLengthKeyWord = totalLengthKeyword + Integer.BYTES + Short.BYTES;

        dictByteBuffer.putInt(totalLengthKeyWord / pfc_dict.PAGE_SIZE);
        dictByteBuffer.putShort((short) (totalLengthKeyWord % pfc_dict.PAGE_SIZE));

        // initialize position
        nextKeywordPos = Integer.BYTES + Short.BYTES;
        nextDictPos = totalLengthKeyWord + Integer.BYTES;

    }

    // allocate number of keyword at the location where dictionary start
    public void allocateNumberOfKeyWord(int szKeyword) {

        dictByteBuffer.position(nextKeywordPos);

        insertInteger(szKeyword, WriteToWhere.To_Dictionary_File);

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
     *         4              4            2            4               4
     * | keyword length | list page | list offset | list length | position metadata location
     */
    public void insertMetaDataSlot(int keyLength, int valueLength, int numberOfDocs) {
        // point
        dictByteBuffer.position(nextDictPos);
        insertInteger(keyLength, WriteToWhere.To_Dictionary_File);
        retrieveLocation(keyWordPos, keyLength, keyWordPos);

        insertInteger(docIDPos.Page, WriteToWhere.To_Dictionary_File);
        insertShort(docIDPos.Offset, WriteToWhere.To_Dictionary_File);
        insertInteger(valueLength, WriteToWhere.To_Dictionary_File);
        retrieveLocation(docIDPos, valueLength, docIDPos);
        insertInteger(docIdCount, WriteToWhere.To_Dictionary_File);

        // update
        nextDictPos += SLOT_SIZE;
        docIdCount += numberOfDocs;
    }

    public void insertPostingList(byte[] lst) {
        insertByte(lst, WriteToWhere.To_Posting_List);
    }

    public void insertPositionList(byte[] lst) {
        insertByte(lst, WriteToWhere.To_Position_List);
        //insert the metadata
        insertInteger( posListPos.Page, WriteToWhere.To_Pos_Meta_File);
        insertShort(posListPos.Offset, WriteToWhere.To_Pos_Meta_File);
        insertInteger(lst.length, WriteToWhere.To_Pos_Meta_File);
        retrieveLocation(posListPos, lst.length, posListPos);
    }

    /**
     * ===== READ =====
     */
    public void readInitiate() {
        // set pointPos
        byteBuffer = pfc_dict.readPage(0);
        pointPos.Page = byteBuffer.getInt();
        pointPos.Offset = byteBuffer.getShort();

        // set byteBuffer to where the dictionary start (the page `pointPos` point at)
        if (pointPos.Page != 0) {
            byteBuffer = pfc_dict.readPage(pointPos.Page);
        }
        byteBuffer.position(pointPos.Offset);

        int szKeyword = readInt(byteBuffer, pointPos, WriteToWhere.To_Dictionary_File);
        retrieveLocation(pointPos, szKeyword * SLOT_SIZE, dictEndPos);

        // set refByteBuffer
        if (pointPos.Page != 0) {
            refByteBuffer = pfc_dict.readPage(0);
        } else {
            refByteBuffer = byteBuffer.duplicate();
        }
        refByteBuffer.position(6);
    }

    public void readPostingInitiate() {
        // set pointPos
        byteBuffer = pfc_posting.readPage(0);
        pointPos.Page = 0;
        pointPos.Offset = 0;


        // set refByteBuffer
        refByteBuffer.clear();
    }

    public void readPositionInitiate() {
        positionByteBuffer = pfc_position.readPage(0);
        posPointPos.Page = 0;
        posPointPos.Offset = 0;
    }

    public void readPositionMetaInitiate() {
        posMetaByteBuffer = pfc_posMeta.readPage(0);
        pfc_posMeta.readCounter--;
        metaPos.Page = 0;
        metaPos.Offset = 0;
    }

    public String readKeywordAndDict(List<Integer> dict) {
        int length = readInt(byteBuffer, pointPos, WriteToWhere.To_Dictionary_File);

        String keyword = readString(length, refByteBuffer, keyWordPos, WriteToWhere.To_Dictionary_File);

        int docPg = readInt(byteBuffer, pointPos, WriteToWhere.To_Dictionary_File);
        short docOffset = readShort(byteBuffer, pointPos, WriteToWhere.To_Dictionary_File);
        int docLength = readInt(byteBuffer, pointPos, WriteToWhere.To_Dictionary_File);
        int positionListMetadaLocation = readInt(byteBuffer, pointPos, WriteToWhere.To_Dictionary_File);
        dict.add(docPg);
        dict.add((int) docOffset);
        dict.add(docLength);
        dict.add(positionListMetadaLocation);

        return keyword;
    }

    public Map<Integer, List<Integer>> readDocIdList(int pageNum, int listOffset, int docIdLength, int positionSlot) {
        Map<Integer, List<Integer>> docIdList = new TreeMap<>();
        byte[] bytes = new byte[docIdLength];
        Location loc = new Location(pageNum, listOffset);
        if (pointPos.Page != loc.Page) {
            byteBuffer = pfc_posting.readPage(loc.Page);

            pointPos.Page += 1;
            pointPos.Offset = loc.Offset;
            byteBuffer.position(pointPos.Offset);

        }
        ByteBuffer newBb = readByte(byteBuffer, loc, byteBuffer.remaining(), docIdLength, bytes, WriteToWhere.To_Posting_List);
        byteBuffer = newBb;
        List<Integer> docIds;
        if (isPositional()) {
            docIds = compressor.decode(bytes);
        } else {
            docIds = new NaiveCompressor().decode(bytes);
        }
        for (int i = 0; i < docIds.size(); i++) {
            List<Integer> positionListMetaData = new ArrayList<>();
            if (isPositional()) {
                loc = new Location(positionSlot * POSITION_SLOT_SIZE /
                        pfc_posMeta.PAGE_SIZE, positionSlot * POSITION_SLOT_SIZE % pfc_posMeta.PAGE_SIZE);
                positionListMetaData.add(readInt(posMetaByteBuffer, loc, WriteToWhere.To_Pos_Meta_File));
                positionListMetaData.add((int) readShort(posMetaByteBuffer, loc, WriteToWhere.To_Pos_Meta_File));
                positionListMetaData.add(readInt(posMetaByteBuffer, loc, WriteToWhere.To_Pos_Meta_File));
            }
            docIdList.put(docIds.get(i), positionListMetaData);
            positionSlot++;
        }
        //if positional get the meta data per element

        return docIdList;
    }

    public List<Integer> readPosList(int pageNum, int listOffset, int posListSize) {
        byte[] bytes = new byte[posListSize];
        Location loc = new Location(pageNum, listOffset);
        if (posPointPos.Page != loc.Page) {
            positionByteBuffer = pfc_position.readPage(loc.Page);

            posPointPos.Page += 1;
            posPointPos.Offset = loc.Offset;
            positionByteBuffer.position(posPointPos.Offset);

        }
        ByteBuffer newBb = readByte(positionByteBuffer, loc, positionByteBuffer.remaining(), posListSize, bytes, WriteToWhere.To_Position_List);
        positionByteBuffer = newBb;
        return compressor.decode(bytes);
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
        byte[] bytes = ByteBuffer.allocate(Short.BYTES).putShort(sh).array();
        byte[] byteA = new byte[pivot];
        byte[] byteB = new byte[Short.BYTES - pivot];

        for (int it = 0; it < pivot; ++it)
            byteA[it] = bytes[it];
        for (int it = pivot; it < Short.BYTES; ++it)
            byteB[it - pivot] = bytes[it];

        return new Pair(byteA, byteB);
    }

    private Pair<byte[], byte[]> splitByteToTwo(int remaining, byte[] bytes) {
        byte[] byteA = new byte[remaining];
        byte[] byteB = new byte[bytes.length - remaining];

        System.arraycopy(bytes, 0, byteA, 0, remaining);
        System.arraycopy(bytes, remaining, byteB, 0, bytes.length - remaining);
        return new Pair<>(byteA, byteB);
    }

    public String readString(int len, ByteBuffer bb, Location lc, WriteToWhere writeWhere) {
        byte[] b = new byte[len];
        ByteBuffer newBb = readByte(bb, lc, pfc_dict.PAGE_SIZE - lc.Offset, len, b, writeWhere);

        // determine which byteBuffer we should assign to
        refByteBuffer = newBb;

        String str = new String(b);
        return str;
    }

    public short readShort(ByteBuffer bb, Location lc, WriteToWhere writeWhere) {
        byte[] b = new byte[Short.BYTES];
        if (writeWhere == WriteToWhere.To_Pos_Meta_File) {
            if (metaPos.Page != lc.Page) {
                bb = pfc_posMeta.readPage(lc.Page);
                pfc_posMeta.readCounter--;
                metaPos.Page += 1;
                metaPos.Offset = lc.Offset;
                posMetaByteBuffer.position(metaPos.Offset);

            }
            //implement equals location
            ByteBuffer newBb = readByte(bb, lc, posMetaByteBuffer.remaining(), Short.BYTES, b, writeWhere);

            posMetaByteBuffer = newBb;
        } else {
            if (pointPos.Page != lc.Page) {
                bb = pfc_dict.readPage(lc.Page);

                pointPos.Page += 1;
                pointPos.Offset = lc.Offset;
                byteBuffer.position(pointPos.Offset);
            }
            //implement equals location
            ByteBuffer newBb = readByte(bb, lc, byteBuffer.remaining(), Short.BYTES, b, writeWhere);

            byteBuffer = newBb;
        }
        Short sh = ByteBuffer.wrap(b).getShort(); // https://stackoverflow.com/questions/7619058/convert-a-byte-array-to-integer-in-java-and-vice-versa
        return sh;
    }

    public int readInt(ByteBuffer bb, Location lc, WriteToWhere writeWhere) {
        if (writeWhere == WriteToWhere.To_Pos_Meta_File) {
            if (metaPos.Page != lc.Page) {
                bb = pfc_posMeta.readPage(lc.Page);
                pfc_posMeta.readCounter--;
                metaPos.Page += 1;
                metaPos.Offset = lc.Offset;
                posMetaByteBuffer.position(metaPos.Offset);
            }
            //implement equals location
            byte[] b = new byte[Integer.BYTES];
            ByteBuffer newBb = readByte(bb, lc, posMetaByteBuffer.remaining(), Integer.BYTES, b, writeWhere);

            posMetaByteBuffer = newBb;
            int i = ByteBuffer.wrap(b).getInt();
            return i;
        } else {
            if (pointPos.Page != lc.Page) {
                bb = pfc_dict.readPage(lc.Page);

                pointPos.Page += 1;
                pointPos.Offset = lc.Offset;
                byteBuffer.position(pointPos.Offset);

            }
            //implement equals location
            byte[] b = new byte[Integer.BYTES];
            ByteBuffer newBb = readByte(bb, lc, byteBuffer.remaining(), Integer.BYTES, b, writeWhere);

            byteBuffer = newBb;

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
        for (int i = 0; i < Math.min(disToEnd, length); ++i) {
            concat[p++] = bb.get();
        }

        lc.Offset += Math.min(disToEnd, length);

        // if the distance is enough to read all the bytes without reading from the next page, return byte array
        if (disToEnd >= length) return bb;

        int newLength = length - disToEnd;
do{
    int subLength = Math.min(newLength, pfc_posting.PAGE_SIZE);
        // new page
        lc.Page += 1;
        lc.Offset = 0;
        bb.clear();

        if (writeWhere == WriteToWhere.To_Dictionary_File) {
            bb = pfc_dict.readPage(lc.Page);
        } else if (writeWhere == WriteToWhere.To_Posting_List) {
            bb = pfc_posting.readPage(lc.Page);
        } else if (writeWhere == WriteToWhere.To_Position_List) {
            bb = pfc_position.readPage(lc.Page);
        } else {
            bb = pfc_posMeta.readPage(lc.Page);
            pfc_posMeta.readCounter--;
        }

        for (int i = 0; i < subLength; i++) {
            concat[p++] = bb.get();
        }

        // set lc offset
        lc.Offset += subLength;
        newLength -= subLength;
    }while(newLength > pfc_posting.PAGE_SIZE);
        return bb;
    }

    public void insertString(String str) {

        byte[] byteStr = str.getBytes();
        dictByteBuffer.put(byteStr);
    }

    public void insertShort(short sh, WriteToWhere writeWhere) {
        if (writeWhere == WriteToWhere.To_Pos_Meta_File) {
            if (Short.BYTES > posMetaByteBuffer.remaining()) {
                // split into 2 substring and insert into page respectively
                Pair<byte[], byte[]> byteP = splitShortToByte(posMetaByteBuffer.remaining(), sh);

                // allocate two byte array
                allocateBytePair(byteP, writeWhere);
            } else {
                posMetaByteBuffer.putShort(sh);
                metaPos.Offset += Short.BYTES;
            }
        } else {
            dictByteBuffer.putShort(sh);
        }
    }

    private void insertByte(byte[] bytes, WriteToWhere writeWhere) {
        int remaining = byteBuffer.remaining();
        if (writeWhere == WriteToWhere.To_Position_List) {
            remaining = positionByteBuffer.remaining();

        }
        if (bytes.length > remaining) {

            Pair<byte[], byte[]> byteP = splitByteToTwo(remaining, bytes);
            // allocate two byte array
            allocateBytePair(byteP, writeWhere);
        } else {
            if (writeWhere == WriteToWhere.To_Posting_List) {
                byteBuffer.put(bytes);
                pointPos.Offset += bytes.length;
            } else {
                positionByteBuffer.put(bytes);
                posPointPos.Offset += bytes.length;
            }
        }
    }

    public void insertInteger(int i, WriteToWhere writeWhere) {
        if (writeWhere == WriteToWhere.To_Pos_Meta_File) {
            if (Integer.BYTES > posMetaByteBuffer.remaining()) {
                // split into 2 substring and insert into page respectively
                Pair<byte[], byte[]> byteP = splitIntegerToByte(posMetaByteBuffer.remaining(), i);

                // allocate two byte array
                allocateBytePair(byteP, writeWhere);
            } else {
                posMetaByteBuffer.putInt(i);
                metaPos.Offset += Integer.BYTES;
            }
        } else {
            dictByteBuffer.putInt(i);
        }
    }


    // allocate byte pair and update point position
    public void allocateBytePair(Pair<byte[], byte[]> byteP, WriteToWhere writeToWhere) {
        if (writeToWhere == WriteToWhere.To_Posting_List) {
            byteBuffer.put(byteP.getKey());

            // append page and update point position
            pfc_posting.appendPage(byteBuffer);
            pointPos.Page += 1;
            pointPos.Offset = 0;

            // insert
            byteBuffer.clear();
            byteBuffer = ByteBuffer.allocate(pfc_posting.PAGE_SIZE);
            if(byteP.getValue().length > pfc_posting.PAGE_SIZE){
                allocateBytePair(splitByteToTwo(byteBuffer.remaining(),byteP.getValue()), writeToWhere);
                return;
            }
            byteBuffer.put(byteP.getValue());
            pointPos.Offset += byteP.getValue().length;
        }
        else if (writeToWhere == WriteToWhere.To_Position_List) {
            positionByteBuffer.put(byteP.getKey());

            // append page and update point position
            pfc_position.appendPage(positionByteBuffer);
            posPointPos.Page += 1;
            posPointPos.Offset = 0;

            // insert
            positionByteBuffer.clear();
            positionByteBuffer = ByteBuffer.allocate(pfc_position.PAGE_SIZE);
            if(byteP.getValue().length > pfc_position.PAGE_SIZE){
                allocateBytePair(splitByteToTwo(positionByteBuffer.remaining(),byteP.getValue()), writeToWhere);
                return;
            }
            positionByteBuffer.put(byteP.getValue());
            posPointPos.Offset += byteP.getValue().length;
        }
        else {
            posMetaByteBuffer.put(byteP.getKey());

            // append page and update point position
            pfc_posMeta.appendPage(posMetaByteBuffer);
            pfc_posMeta.writeCounter--;
            metaPos.Page += 1;
            metaPos.Offset = 0;

            // insert
            posMetaByteBuffer.clear();
            posMetaByteBuffer = ByteBuffer.allocate(pfc_posMeta.PAGE_SIZE);
            if(byteP.getValue().length > pfc_posMeta.PAGE_SIZE){
                allocateBytePair(splitByteToTwo(posMetaByteBuffer.remaining(),byteP.getValue()), writeToWhere);
                return;
            }
            posMetaByteBuffer.put(byteP.getValue());
            metaPos.Offset += byteP.getValue().length;
        }
    }


    public boolean hasKeyWord() {
        return !(pointPos.Page == dictEndPos.Page && pointPos.Offset == dictEndPos.Offset) && !(pointPos.Page == dictEndPos.Page -1 && pointPos.Offset == pfc_dict.PAGE_SIZE && dictEndPos.Offset == 0);
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
        if (isPositional()) {
            pfc_position.appendPage(positionByteBuffer);
            pfc_posMeta.appendPage(posMetaByteBuffer);
            pfc_posMeta.writeCounter--;
        }
    }

    public void appendAllbyte() {
        pfc_dict.appendAllBytes(dictByteBuffer);
    }

    public void close() {
        pfc_dict.close();
        pfc_posting.close();
        if (isPositional()) {
            pfc_position.close();
            pfc_posMeta.close();
        }
    }

    private boolean isPositional() {
        return compressor != null;
    }

}