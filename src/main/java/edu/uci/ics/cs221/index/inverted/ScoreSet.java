package edu.uci.ics.cs221.index.inverted;

public class ScoreSet implements Comparable<ScoreSet> {
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
}
