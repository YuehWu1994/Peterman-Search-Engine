package edu.uci.ics.cs221.index.inverted;

import java.util.ArrayList;
import java.util.List;

public class Node {
    private List<Integer> incoming;
    private int outgoingSize;
    private double prevScore;
    private double currentScore;

    public Node(){
        incoming = new ArrayList<>();
        outgoingSize = 0;
        prevScore = 1;
        currentScore = 0;
    }
    public Node(List<Integer> incoming, int outgoingSize, double prevScore, double currentScore) {
        this.incoming = incoming;
        this.outgoingSize = outgoingSize;
        this.prevScore = prevScore;
        this.currentScore = currentScore;
    }

    public double getPrevScore() {
        return prevScore;
    }

    public void setPrevScore(double prevScore) {
        this.prevScore = prevScore;
    }

    public double getCurrentScore() {
        return currentScore;
    }

    public void setCurrentScore(double currentScore) {
        this.currentScore = currentScore;
    }

    public List<Integer> getIncoming() {
        return incoming;
    }

    public void setIncoming(List<Integer> incoming) {
        this.incoming = incoming;
    }

    public int getOutgoingSize() {
        return outgoingSize;
    }

    public void setOutgoingSize(int outgoingSize) {
        this.outgoingSize = outgoingSize;
    }

    public boolean addIncomingNode(Integer i){
        return incoming.add(i);
    }

}
