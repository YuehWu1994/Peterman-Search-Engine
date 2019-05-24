package edu.uci.ics.cs221.index.inverted;

public class Location {
    public int Page;
    public short Offset;

    Location(int page, int offset){
        Page = page;
        Offset = (short) offset;
    }


}
