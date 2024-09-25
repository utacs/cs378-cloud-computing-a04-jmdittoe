package edu.cs.utexas.task2;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class IdAndError implements Comparable<IdAndError> {

    private final Text taxId;
    private final FloatWritable error;

    public IdAndError(Text taxiId, FloatWritable error) {
        this.taxId = taxiId;
        this.error = error;
    }

    public Text getId() {
        return taxId;
    }

    public FloatWritable getError() {
        return error;
    }
    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
    @Override
    public int compareTo(IdAndError other) {

        float diff = error.get() - other.error.get();
        if (diff > 0) {
            return 1;
        } else if (diff < 0) {
            return -1;
        }
        return 0;
    }


    public String toString(){

        return "("+taxId.toString() +" , "+ error.toString()+")";
    }
}

