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

    public static int SLOT_SIZE = 8;


    /**
     * Define the location where we start storing dictionary
     */
    private static Location dictionaryStart;

    /**
     * Define the location where we point to docID, keyword
     */
    private Location keyWordPos = new Location(0,Integer.BYTES);
    private Location docIDPos = new Location(0,0);

    /**
     * Define the location where we point for insersion
     */
    private Location pointPos = new Location(0,0);




    SegmentInDiskManager(Path path) {
        pfc = PageFileChannel.createOrOpen(path);

        byteBuffer = ByteBuffer.allocate(pfc.PAGE_SIZE);
    }


    public void insertKeyWord(String str){
        insertString(str);
    }

    /**
     * | keywordPage | keyword offset | list page | list offset |
     */
    public void insertMetaDataSlot(int keyLength, int valueLength){
        insertShort(keyWordPos.Page);
        insertShort(keyWordPos.Offset);
        retrieveLocation(keyWordPos, keyLength, keyWordPos); // not sure

        insertShort(docIDPos.Page);
        insertShort(docIDPos.Offset);
        retrieveLocation(docIDPos, valueLength, docIDPos); // not sure
    }

    public void insertListOfDocID(List<Integer> lst){
        for(Integer i : lst) insertInteger(i);
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


    public void allocateDictStart(int totalLengthKeyword) {
        int totalLengthKeyWord = totalLengthKeyword + Integer.BYTES;

        dictionaryStart.Page = (short)(totalLengthKeyWord/pfc.PAGE_SIZE);
        dictionaryStart.Offset = (short)(totalLengthKeyWord%pfc.PAGE_SIZE);

        byteBuffer.putShort(dictionaryStart.Page);
        byteBuffer.putShort(dictionaryStart.Offset);

        pointPos.Offset += Integer.BYTES;
    }

    public void allocateNumberOfKeyWord(int szKeyword) {
        // assign docID Position
        retrieveLocation(dictionaryStart, Integer.BYTES + SLOT_SIZE*szKeyword, docIDPos);


        if(Integer.BYTES > byteBuffer.remaining()){
            Pair<byte[] , byte[]> byteP = splitIntegerToByte(byteBuffer.remaining(), szKeyword);
            allocateBytePair(byteP);
        }
        else{
            byteBuffer.put((byte)szKeyword);
            pointPos.Offset += Integer.BYTES;
        }
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


    public String toKeyword(int idx){
        return "test";
    }

    public List<Integer> toDocList(int idx){
        return new ArrayList<>();
    }
}
