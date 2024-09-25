package edu.cs.utexas.task2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Iterator;



public class TopGPSErrorsTopKReducer extends  Reducer<Text, FloatWritable, Text, FloatWritable> {

    private PriorityQueue<IdAndError> pq = new PriorityQueue<IdAndError>(5);;


    private Logger logger = Logger.getLogger(TopGPSErrorsTopKReducer.class);


    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {


        // A local counter just to illustrate the number of values here!
        int counter = 0 ;


        // size of values is 1 because key only has one distinct value
        for (FloatWritable value : values) {
            counter = counter + 1;

            pq.add(new IdAndError(new Text(key), new FloatWritable(value.get()) ) );

            logger.info("Reducer Text: " + key.toString() + " , Count: " + value.toString());
            logger.info("PQ Status: " + pq.toString());
        }

        // keep the priorityQueue size <= heapSize
        while (pq.size() > 5) {
            pq.poll();
        }


    }


    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<IdAndError> values = new ArrayList<IdAndError>(5);

        while (!pq.isEmpty()) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());


        // reverse so they are ordered in descending order
        //Collections.reverse(values);


        for (IdAndError value : values) {
            context.write(value.getId(), value.getError());
        }


    }

}