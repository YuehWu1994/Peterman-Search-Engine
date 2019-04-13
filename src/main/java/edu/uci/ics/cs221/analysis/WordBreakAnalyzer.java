package edu.uci.ics.cs221.analysis;

import java.util.ArrayList;
import java.util.List;

public class WordBreakAnalyzer implements Comparable<WordBreakAnalyzer>{
    private List<Integer> splitPos;
    private double probability;

    public WordBreakAnalyzer()
    {
        splitPos = new ArrayList<>();
        probability = 0;
    }
    public WordBreakAnalyzer(List<Integer> splitPos, double probability)
    {
        this.splitPos = splitPos;
        this.probability = probability;
    }

    public double getProbability() {
        return probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }

    public List<Integer> getSplitPos() {
        return splitPos;
    }

    public void setSplitPos(List<Integer> splitPos) {
        this.splitPos = splitPos;
    }
    public boolean addPosition(int i)
    {
        return splitPos.add(i);
    }

    @Override
    public int compareTo(WordBreakAnalyzer o) {
        if(getProbability() < o.getProbability())
            return -1;
        else if(o.getProbability() < getProbability())
            return 1;
        return 0;
    }
}
