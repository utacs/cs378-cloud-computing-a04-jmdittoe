package edu.cs.utexas.task2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopGPSErrorsWrapper implements Writable {

    //To represent the taxi record itself
    FloatWritable itself;
    //To represent if this taxi record is an error(0 - no problem, 1 - there was a problem)
    IntWritable error;

    public TopGPSErrorsWrapper() {
        itself = new FloatWritable(1);
        error = new IntWritable(1);
    }

    public TopGPSErrorsWrapper(int value, float arrival) {
        itself = new FloatWritable(arrival);
        error = new IntWritable(value);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        itself.write(dataOutput);
        error.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        itself.readFields(dataInput);
        error.readFields(dataInput);
    }

    public void changeToError(){
        error = new IntWritable(1);
    }

    public void changeToNoError(){
        error = new IntWritable(0);
    }


    public float getItself(){
        return itself.get();
    }

    public int getError(){
        return error.get();
    }

    public String toString(){
        return itself.toString() + " " + error.toString();
    }


}
