package edu.cs.utexas.task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class GPSErrorCountReducer extends  Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

   public void reduce(IntWritable hour, Iterable<IntWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       int sum = 0;
       
       for (IntWritable value : values) {
           sum += value.get();
       }
       
       context.write(hour, new IntWritable(sum));
   }
}