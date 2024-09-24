package edu.cs.utexas.task3;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaxiReducer extends  Reducer<Text, TaxiClass, Text, FloatWritable> {

    public void reduce(Text text, Iterable<TaxiClass> values, Context context)
            throws IOException, InterruptedException {

        int seconds = 0;
        Float money = 0f;

        for (TaxiClass value : values) {
            seconds += value.getSeconds();
            money += value.getMoney();
        }

        float moneyPerMinute = money / ((float) seconds / 60);

        context.write(text, new FloatWritable(moneyPerMinute));
    }
}