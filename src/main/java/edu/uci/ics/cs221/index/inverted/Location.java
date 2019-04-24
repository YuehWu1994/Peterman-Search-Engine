package edu.uci.ics.cs221.index.inverted;

public class Location {
    public short Page;
    public short Offset;

    Location(int page, int offset){
        Page = (short) page;
        Offset = (short) offset;
    }


}
