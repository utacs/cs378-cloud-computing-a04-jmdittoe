package edu.cs.utexas.task3;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;


public class TopTaxiReducer extends  Reducer<Text, FloatWritable, Text, FloatWritable> {

    private PriorityQueue<TaxiAndRate> pq = new PriorityQueue<TaxiAndRate>(10);;


    private Logger logger = Logger.getLogger(TopTaxiReducer.class);


//    public void setup(Context context) {
//
//        pq = new PriorityQueue<WordAndCount>(10);
//    }


    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * @param key
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
            logger.info("Reducer Text: counter is " + counter);
            logger.info("Reducer Text: Add this item  " + new TaxiAndRate(key, value).toString());

            pq.add(new TaxiAndRate(new Text(key), new FloatWritable(value.get()) ) );

            logger.info("Reducer Text: " + key.toString() + " , Count: " + value.toString());
            logger.info("PQ Status: " + pq.toString());
        }

        // keep the priorityQueue size <= heapSize
        while (pq.size() > 10) {
            pq.poll();
        }


    }


    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<TaxiAndRate> values = new ArrayList<TaxiAndRate>(10);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());


        // reverse so they are ordered in descending order
        Collections.reverse(values);


        for (TaxiAndRate value : values) {
            context.write(value.getDriver(), value.getRate());
            logger.info("TopKReducer - Top-10 Rates are:  " + value.getDriver() + "  Count:"+ value.getRate());
        }


    }

}