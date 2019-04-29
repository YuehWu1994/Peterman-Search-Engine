package edu.uci.ics.cs221.index.inverted;


import java.nio.file.Path;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.List;
import javafx.util.Pair;

/**
 * | key word | dictionary (4-byte keyword length + 12-byte 1 keyword slot) | docID list
 * |           key word   + dictionary                                      |
 *
 */


public class SegmentInDiskManager {

    PageFileChannel pfc;
    ByteBuffer byteBuffer;
    ByteBuffer refByteBuffer; // this byte buffer is used to read keyword or docId

    public static int SLOT_SIZE = 16;


    /*
     * Define the location where we start storing dictionary
     */
    private static Location dictionaryStart;

    /*
     * Define the location where we point to docID, keyword
     */
    private Location keyWordPos;
    private Location docIDPos;

    /*
     * Define the location where we point for insersion/ read
     */
    private Location pointPos;




    SegmentInDiskManager(Path path) {
        pfc = PageFileChannel.createOrOpen(path);

        byteBuffer = ByteBuffer.allocate(pfc.PAGE_SIZE);

        keyWordPos = new Location(0,Integer.BYTES*2);
        pointPos = new Location(0,0);
        docIDPos = new Location(0,0);
    }

    /**
     *  ===== INSERT =====
     */

    public void insertKeyWord(String str){
        insertString(str);
    }

    /*
     * | keywordPage | keyword offset | keyword length | list page | list offset | list length
     */
    public void insertMetaDataSlot(int keyLength, int valueLength){
        insertShort(keyWordPos.Page);
        insertShort(keyWordPos.Offset);
        insertInteger(keyLength);
        retrieveLocation(keyWordPos, keyLength, keyWordPos); // not sure

        insertShort(docIDPos.Page);
        insertShort(docIDPos.Offset);
        insertInteger(valueLength);
        retrieveLocation(docIDPos, valueLength, docIDPos); // not sure
    }

    public void insertListOfDocID(List<Integer> lst){
        for(Integer i : lst) insertInteger(i);
    }

    /**
     *  ===== READ =====
     */
    public void readInitiate(){
        // set pointPos
        byteBuffer = pfc.readPage(0);
        pointPos.Page = byteBuffer.getShort();
        pointPos.Offset = byteBuffer.getShort();

        // set byteBuffer to where the dictionary start (the page `pointPos` point at)
        byteBuffer = pfc.readPage(pointPos.Page); // not sure
        byteBuffer.position(pointPos.Offset);

        // set docIDPos
        int szKeyword = readInt(byteBuffer, pointPos);
        retrieveLocation(pointPos, szKeyword*SLOT_SIZE, docIDPos);

        // set refByteBuffer
        refByteBuffer = pfc.readPage(0);
    }

    public String readKeywordAndDict(List<Integer> dict){
        short pg = readShort(byteBuffer, pointPos);
        short offset = readShort(byteBuffer, pointPos);
        int length = readInt(byteBuffer, pointPos);

        String keyword = readString(length, refByteBuffer, keyWordPos);

        short docPg = readShort(byteBuffer, pointPos);
        short docOffset = readShort(byteBuffer, pointPos);
        int docLength = readInt(byteBuffer, pointPos);

        dict.add((int)docPg);
        dict.add((int)docOffset);
        dict.add(docLength);

        return keyword;
    }

    public List<Integer> readDocIdList(int docIdLength){
        List<Integer> docIdList = new ArrayList<>();
        int docSz = docIdLength/Integer.BYTES;
        for(int i = 0; i < docSz; ++i){
            docIdList.add(readInt(byteBuffer, pointPos));
        }
        return docIdList;
    }

    /**
     *  ===== Page Utility =====
     */
    public Pair<byte[] , byte[]>  splitShortToByte(int pivot, short sh){
        byte[] bytes = ByteBuffer.allocate(Integer.BYTES).putInt(sh).array();
        byte[] byteA = new byte[pivot];
        byte[] byteB = new byte[Integer.BYTES-pivot];

        for(int it = 0; it < pivot; ++it)  byteA[it] = bytes[it];
        for(int it = pivot; it < Short.BYTES; ++it) byteB[it-pivot] = bytes[it];

        return new Pair(byteA, byteB);
    }

    public Pair<byte[] , byte[]>  splitIntegerToByte(int pivot, int i){
        byte[] bytes = ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
        byte[] byteA = new byte[pivot];
        byte[] byteB = new byte[Integer.BYTES-pivot];

        for(int it = 0; it < pivot; ++it)  byteA[it] = bytes[it];
        for(int it = pivot; it < Integer.BYTES; ++it) byteB[it-pivot] = bytes[it];

        return new Pair(byteA, byteB);
    }

    public Pair<byte[], byte[]> splitStringToByte(int pivot, String str){
        String a = str.substring(0, pivot);
        String b = str.substring(pivot);
        byte[] byteA = a.getBytes();
        byte[] byteB = b.getBytes();

        return new Pair(byteA, byteB);
    }

    public String readString(int len, ByteBuffer bb, Location lc){
        byte [] b = readByte(bb, lc, bb.remaining(), len);
        return new String(b);
    }

    public short readShort(ByteBuffer bb, Location lc){
        byte [] b = readByte(bb, lc, byteBuffer.remaining(), Short.BYTES);
        return ByteBuffer.wrap(b).getShort(); // https://stackoverflow.com/questions/7619058/convert-a-byte-array-to-integer-in-java-and-vice-versa

    }

    public int readInt(ByteBuffer bb, Location lc){
        byte [] b = readByte(bb, lc, byteBuffer.remaining(), Integer.BYTES);
        return ByteBuffer.wrap(b).getInt();
    }

    /**
     *
     * @param disToEnd: distance(byte) from current pointing offset to end of page
     */
    public byte[] readByte(ByteBuffer bb, Location lc, int disToEnd, int length){
        byte [] concat = new byte[length];

        int p = 0;

        for(int i = 0; i < Math.min(disToEnd, length); ++i) concat[p++] = bb.get();

        lc.Offset += Math.min(disToEnd, length);

        // if the distance is enough to read all the bytes without reading from the next page, return byte array
        if(disToEnd >= length) return concat;

        // new page
        lc.Page += 1;
        lc.Offset = 0;

        bb.clear();
        bb = pfc.readPage(lc.Page);

        for(int i = 0; i < length-disToEnd; ++i) concat[p++] = bb.get();

        return concat;
    }

    public void insertString(String str){
        if(str.length() > byteBuffer.remaining()){
            // split into 2 substring and insert into page respectively
            Pair<byte[] , byte[]> byteP = splitStringToByte(byteBuffer.remaining(), str);

            // allocate two byte array
            allocateBytePair(byteP);
        }
        else{
            byte[] byteStr = str.getBytes();
            byteBuffer.put(byteStr);
            pointPos.Offset += str.length();
        }
    }

    public void insertShort(short sh){
        if(Short.BYTES > byteBuffer.remaining()){
            // split into 2 substring and insert into page respectively
            Pair<byte[] , byte[]> byteP = splitShortToByte(byteBuffer.remaining(), sh);

            // allocate two byte array
            allocateBytePair(byteP);
        }
        else{
            byteBuffer.putShort(sh);
            pointPos.Offset += Short.BYTES;
        }
    }

    public void insertInteger(int i){
        if(Integer.BYTES > byteBuffer.remaining()){
            // split into 2 substring and insert into page respectively
            Pair<byte[] , byte[]> byteP = splitIntegerToByte(byteBuffer.remaining(), i);

            // allocate two byte array
            allocateBytePair(byteP);
        }
        else{
            byteBuffer.putInt(i);
            pointPos.Offset += Integer.BYTES;
        }
    }


    // allocate byte pair and update point position
    public void allocateBytePair(Pair<byte[] , byte[]> byteP){
        byteBuffer.put(byteP.getKey());

        // append page and update point position
        pfc.appendPage(byteBuffer);
        pointPos.Page += 1;
        pointPos.Offset = 0;

        // insert
        byteBuffer.clear();
        byteBuffer = ByteBuffer.allocate(pfc.PAGE_SIZE);

        byteBuffer.put(byteP.getValue());
        pointPos.Offset += byteP.getValue().length;
    }


    // allocate the location where we start storing dictionary at the first four bytes of first page
    public void allocateDictStart(int totalLengthKeyword) {
        int totalLengthKeyWord = totalLengthKeyword + Integer.BYTES;

        byteBuffer.putShort((short)(totalLengthKeyWord/pfc.PAGE_SIZE));
        byteBuffer.putShort((short)(totalLengthKeyWord%pfc.PAGE_SIZE));

        pointPos.Offset += Integer.BYTES;
    }

    // allocate number of keyword at the location where dictionary start
    public void allocateNumberOfKeyWord(int szKeyword) {
        // assign docID Position
        retrieveLocation(pointPos, Integer.BYTES + SLOT_SIZE*szKeyword, docIDPos);


        if(Integer.BYTES > byteBuffer.remaining()){
            Pair<byte[] , byte[]> byteP = splitIntegerToByte(byteBuffer.remaining(), szKeyword);
            allocateBytePair(byteP);
        }
        else{
            byteBuffer.put((byte)szKeyword);
            pointPos.Offset += Integer.BYTES;
        }
    }

    // allocate the location where document ID list ends at the 5th~8th byte of first page
    public void allocateDocIDEnd(){
        byteBuffer =  pfc.readPage(0);  // not sure if we should clear

        byteBuffer.position(Integer.BYTES);
        byteBuffer.putShort(docIDPos.Page);
        byteBuffer.putShort(docIDPos.Offset);
        pfc.writePage(0, byteBuffer);
    }


    public void nextDict(){
        retrieveLocation(pointPos, SLOT_SIZE, pointPos);
    }


    public boolean hasKeyWord(){
        return !(pointPos.Page == docIDPos.Page && pointPos.Offset == docIDPos.Offset);
    }


    /**
     *
     * @param loc: original location
     * @param shift: shift by how many bytes
     * @return retrieved location
     */
    public void retrieveLocation(Location loc, int shift, Location loc2){
        // transfer to byte-oriented distance instead of using page
        long dis = loc.Page * pfc.PAGE_SIZE + loc.Offset + shift;

        loc2.Page = (short) (dis/pfc.PAGE_SIZE);
        loc2.Offset = (short) (dis%pfc.PAGE_SIZE);
    }

    public void close(){
        pfc.close();
    }

}
