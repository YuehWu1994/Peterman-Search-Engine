package edu.uci.ics.cs221.index.inverted;


import java.nio.file.Path;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.List;
import javafx.util.Pair;
import java.util.Set;



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

        keyWordPos = new Location(0,Integer.BYTES); // no end of docID, start by (0, 4)
        pointPos = new Location(0,0);
        docIDPos = new Location(0,0);
    }

    /**
     *  ===== INSERT =====
     */

    public void insertKeyWord(String str){
        insertString(str);
        assert (byteBuffer.position() == pointPos.Offset) : "pointer " + pointPos.Offset  + " and buffer position " + byteBuffer.position()+ " not match";
    }

    /*
     * | keywordPage | keyword offset | keyword length | list page | list offset | list length
     */
    public void insertMetaDataSlot(int keyLength, int valueLength){
        insertShort(keyWordPos.Page);
        insertShort(keyWordPos.Offset);
        insertInteger(keyLength);
        retrieveLocation(keyWordPos, keyLength, keyWordPos);

        insertShort(docIDPos.Page);
        insertShort(docIDPos.Offset);
        insertInteger(valueLength);
        retrieveLocation(docIDPos, valueLength, docIDPos);
        assert (byteBuffer.position() == pointPos.Offset) : "pointer " + pointPos.Offset  + " and buffer position " + byteBuffer.position()+ " not match";
        System.out.println("\n");
    }

    public void insertListOfDocID(Set<Integer> lst){
        for(Integer i : lst) insertInteger(i);
        assert (byteBuffer.position() == pointPos.Offset) : "pointer " + pointPos.Offset  + " and buffer position " + byteBuffer.position()+ " not match";
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
        System.out.println("doc ID offset is: " + docIDPos.Offset);

        // set refByteBuffer
        refByteBuffer = pfc.readPage(0);
        refByteBuffer.position(4);
    }

    public String readKeywordAndDict(List<Integer> dict){
        short pg = readShort(byteBuffer, pointPos);
        short offset = readShort(byteBuffer, pointPos);
        int length = readInt(byteBuffer, pointPos);

        String keyword = readString(length, refByteBuffer, keyWordPos);

        short docPg = readShort(byteBuffer, pointPos);
        short docOffset = readShort(byteBuffer, pointPos);
        int docLength = readInt(byteBuffer, pointPos);

        //System.out.println("pointPos is: (" +  pointPos.Page + "," + pointPos.Offset + ")");
        System.out.println("\n");

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

        byte[] byteStr = str.getBytes();
        byte[] byteA = new byte[pivot];
        byte[] byteB = new byte[byteStr.length-pivot];

        for(int i = 0; i < pivot; ++i) byteA[i] = byteStr[i];
        for(int i = 0; i < byteStr.length-pivot; ++i) byteB[i] = byteStr[i+pivot];

        return new Pair(byteA, byteB);
    }

    public String readString(int len, ByteBuffer bb, Location lc){
        System.out.print("Read string at (" + lc.Page + "," + lc.Offset + "), length is: " + len);
        byte [] b = readByte(bb, lc, bb.remaining(), len);
        String str = new String(b);
        System.out.print(", value is: " + str + '\n');
        return str;
    }

    public short readShort(ByteBuffer bb, Location lc){
        System.out.print("Read short at (" + lc.Page + "," + lc.Offset + "), length is: 2");
        byte [] b = readByte(bb, lc, byteBuffer.remaining(), Short.BYTES);
        Short sh = ByteBuffer.wrap(b).getShort(); // https://stackoverflow.com/questions/7619058/convert-a-byte-array-to-integer-in-java-and-vice-versa
        System.out.print(", value is: " + sh + '\n');
        return sh;
    }

    public int readInt(ByteBuffer bb, Location lc){
        System.out.print("Read integer at (" + lc.Page + "," + lc.Offset + "), length is: 4");
        byte [] b = readByte(bb, lc, byteBuffer.remaining(), Integer.BYTES);
        int i = ByteBuffer.wrap(b).getInt();
        System.out.print(", value is: " + i + '\n');
        return i;
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

        //System.out.println(disToEnd);
        //System.out.println(length);

        // if the distance is enough to read all the bytes without reading from the next page, return byte array
        if(disToEnd >= length) return concat;


        // new page
        lc.Page += 1;
        lc.Offset = 0;

        bb.clear();
        bb = pfc.readPage(lc.Page);

        for(int i = 0; i < length-disToEnd; ++i) concat[p++] = bb.get();

        // set lc offset
        lc.Offset += length-disToEnd;

        return concat;
    }

    public void insertString(String str){
        System.out.println("Insert string " + str + " at (" + pointPos.Page + "," + pointPos.Offset + "), length is: " + str.getBytes().length);
        if(str.getBytes().length > byteBuffer.remaining()){
            // split into 2 substring and insert into page respectively
            Pair<byte[] , byte[]> byteP = splitStringToByte(byteBuffer.remaining(), str);

            // allocate two byte array
            allocateBytePair(byteP);
        }
        else{
//            char ch = '-';
//            byte b1 = (byte) ch;      // explicit conversion from char to byte
//            System.out.println(b1);
//            System.out.println(byteBuffer.position());
//            System.out.println(str.length());
            byte[] byteStr = str.getBytes();
//            for(byte b : byteStr) System.out.println(b);
//            System.out.println(byteStr.length);
            byteBuffer.put(byteStr);
//            System.out.println(byteBuffer.position());
            //pointPos.Offset += str.length();
            pointPos.Offset += byteStr.length;

//            int len = str.length();
//            byte [] byteStr2 = new byte[len];
//            for(int i = 0; i < len; ++i) byteStr2[i] = (byte)str.charAt(i);
//
//            String k = new String(byteStr2);
//            System.out.println(k);

        }
    }

    public void insertShort(short sh){
        System.out.println("Insert short " + sh + " at (" + pointPos.Page + "," + pointPos.Offset + "), length is: 2");
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
        System.out.println("Insert integer " + i + " at (" + pointPos.Page + "," + pointPos.Offset + "), length is: 4");
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
    public void allocateKeywordStart(int totalLengthKeyword) {
        int totalLengthKeyWord = totalLengthKeyword + Integer.BYTES;

        byteBuffer.putShort((short)(totalLengthKeyWord/pfc.PAGE_SIZE));
        byteBuffer.putShort((short)(totalLengthKeyWord%pfc.PAGE_SIZE));


        pointPos.Offset += Integer.BYTES;
        System.out.println("====== Keyword initial position is: " +  pointPos.Page + ' ' + pointPos.Offset + " ====== " );
    }

    // allocate number of keyword at the location where dictionary start
    public void allocateNumberOfKeyWord(int szKeyword) {
        System.out.println("====== Dictionary initial position is: " +  pointPos.Page + ' ' + pointPos.Offset+ " ====== " );

        // assign docID Position
        retrieveLocation(pointPos, Integer.BYTES + SLOT_SIZE*szKeyword, docIDPos);

        System.out.println("====== Doc Id initial position is: " +  docIDPos.Page + ' ' + docIDPos.Offset+ " ====== " );


        if(Integer.BYTES > byteBuffer.remaining()){
            Pair<byte[] , byte[]> byteP = splitIntegerToByte(byteBuffer.remaining(), szKeyword);
            allocateBytePair(byteP);
        }
        else{
            byteBuffer.putInt(szKeyword);
            pointPos.Offset += Integer.BYTES;
        }
    }

//    // allocate the location where document ID list ends at the 5th~8th byte of first page
//    public void allocateDocIDEnd(){
//        byteBuffer =  pfc.readPage(0);  // not sure if we should clear
//
//        byteBuffer.position(Integer.BYTES);
//        byteBuffer.putShort(docIDPos.Page);
//        byteBuffer.putShort(docIDPos.Offset);
//        pfc.writePage(0, byteBuffer);
//    }




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

    public void appendPage(){
        pfc.appendPage(byteBuffer);
    }

    public void close(){
        pfc.close();
    }

}
