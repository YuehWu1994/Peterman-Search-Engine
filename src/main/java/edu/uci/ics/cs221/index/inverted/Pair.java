package edu.uci.ics.cs221.index.inverted;

import edu.uci.ics.cs221.storage.Document;

import javax.print.Doc;
import java.util.Objects;

public class Pair<L, R> implements Comparable<Pair<L, R>>{

    private final L left;
    private final R right;

    public static <L, R> Pair<L, R> of(L left, R right) {
        return new Pair<>(left, right);
    }

    public Pair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public L getLeft() {
        return left;
    }

    public R getRight() {
        return right;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(left, pair.left) &&
                Objects.equals(right, pair.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

    @Override
    public String toString() {
        return "Pair{" +
                "left=" + left +
                ", right=" + right +
                '}';
    }

    @Override
    public int compareTo(Pair<L, R> o) {
        if(!(getRight() instanceof Double)){
            throw new UnsupportedOperationException();
        }
        if((Double) getRight() < (Double) o.getRight())
            return -1;
        else if((Double) o.getRight() < (Double) getRight())
            return 1;
 /*       if(getLeft() instanceof Document){
            return compareDoc((Document) getLeft(), (Document) o.getLeft());
        }
        if(getLeft() instanceof Integer){
            return compareInt((Integer) getLeft(), (Integer) o.getLeft());
        }
        return 0;
    }

    private int compareDoc(Document d1, Document d2){
        int doc1 = Integer.parseInt(d1.getText().substring(0, d1.getText().indexOf("\n")));
        int doc2 = Integer.parseInt(d2.getText().substring(0, d2.getText().indexOf("\n")));
        if(doc1 < doc2) {
            return -1;
        }
        if(doc2 < doc1){
            return 1;
        }
        return 0;
    }

    private int compareInt(Integer i1, Integer i2){
        if(i1 < i2) {
            return -1;
        }
        if(i2 < i1){
            return 1;
        }*/
        return 0;
    }

}
