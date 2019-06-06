package edu.uci.ics.cs221.index.inverted;

public class DocID {
    public int SegmentID;
    public int LocalDocID;

    DocID(int segmentID, int localDocID){
        SegmentID = segmentID;
        LocalDocID =  localDocID;
    }
}
