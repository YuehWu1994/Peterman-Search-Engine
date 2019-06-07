package edu.uci.ics.cs221.index.inverted;

import java.util.Comparator;

public class ScoreSet implements Comparable<ScoreSet>, Comparator<ScoreSet> {
    public double Score;
    public DocID Doc;

    public ScoreSet(Double score, DocID doc) {
        this.Score = score;
        this.Doc = doc;
    }

    // not sure the priority !!!
    @Override
    public int compareTo(ScoreSet other) {
        if(this.Score < other.Score) return -1;
        if(this.Score > other.Score) return 1;
        return 0;
    }


    @Override
    public int compare(ScoreSet o1, ScoreSet o2) {
        return o1.compareTo(o2);
    }
}
