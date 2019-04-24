package edu.uci.ics.cs221.index.inverted;


import java.nio.file.Path;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * | key word | metadata (4-byte keyword length + 12-byte 1 keyword slot) | docID list
 * |           key word   + metadata    =    Dictionary                   |
 *
 */


public class SegmentInDiskManager {

    PageFileChannel pfc;

    /**
     * Define the last position of list
     */
    private Location lastListPos;


    /**
     * Define the location where we start storing meta data
     */
    private static Location metaDataStart;

    /**
     * count which page to start storing list of docID (I store list docID at the beginning of page)
     */
    private static Location docListStart;

    ByteBuffer byteBuffer;

    private Location pointPos = new Location(0,0);

    SegmentInDiskManager(Path path, int leng, int keyWordSize) {
        pfc = PageFileChannel.createOrOpen(path);


        // allocate page & store dictLengt at the first four bytes
        byteBuffer = ByteBuffer.allocate(pfc.PAGE_SIZE);
        byteBuffer.putInt(dictLength);
        pointPos.Offset += Integer.BYTES;
    }


    public void insertKeyWord(String str){
        if(str.length() < byteBuffer.remaining()){
            pfc.appendPage(byteBuffer);
            pointPos.Page += 1;
            pointPos.Offset = 0;
        }
        else{
            byte[] byteStr = str.getBytes();
            byteBuffer.put(byteStr);
            pointPos.Offset += str.length();
        }

    }

    public void insertMetaDataSlot(int keyLength, int valueLength){
    }

    public void insertListOfDocID(List<Integer> lst){

    }


    public void setLastListPosition(int pos) {

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
