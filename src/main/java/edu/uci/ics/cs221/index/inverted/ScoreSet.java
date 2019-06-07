package edu.uci.ics.cs221.index.inverted;

public class ScoreSet implements Comparable<ScoreSet>{
    public double Score;
    public DocID Doc;

    public ScoreSet(Double score, DocID doc) {
        this.Score = score;
        this.Doc = doc;
    }

    // not sure the priority !!!
    @Override
    public int compareTo(ScoreSet other) {
        return Double.compare(this.Score, other.Score);
    }

}
