package edu.cs.utexas.task2;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopGPSErrorsReducer extends  Reducer<Text, TopGPSErrorsWrapper, Text, FloatWritable> {

    public void reduce(Text text, Iterable<TopGPSErrorsWrapper> values, Context context)
            throws IOException, InterruptedException {

        int sumError = 0;
        float sumTotal = 0;

        for (TopGPSErrorsWrapper value : values) {
            sumError += value.getError();
            sumTotal += value.getItself();
        }


        context.write(text, new FloatWritable(sumError/sumTotal));
    }
}