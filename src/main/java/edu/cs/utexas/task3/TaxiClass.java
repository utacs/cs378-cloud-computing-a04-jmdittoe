package edu.cs.utexas.task3;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaxiClass implements Writable {

    private final IntWritable seconds;
    private final FloatWritable money;

    public TaxiClass(){
        this.seconds = new IntWritable(0);
        this.money = new FloatWritable(0);
    }

    public TaxiClass(int s, float f) {
        seconds = new IntWritable(s);
        money = new FloatWritable(f);
    }

    public int getSeconds() {
        return this.seconds.get();
    }

    public float getMoney() {
        return this.money.get();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        seconds.write(dataOutput);
        money.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        seconds.readFields(dataInput);
        money.readFields(dataInput);
    }

    public String toString() {
        return seconds.get() + "," + money.get();
    }

}
