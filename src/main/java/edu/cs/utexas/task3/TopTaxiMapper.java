package edu.cs.utexas.task3;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class TopTaxiMapper extends Mapper<Text, Text, Text, FloatWritable> {

    private Logger logger = Logger.getLogger(TopTaxiMapper.class);


    private PriorityQueue<TaxiAndRate> pq;

    public void setup(Context context) {
        pq = new PriorityQueue<TaxiAndRate>();

    }

    /**
     * Reads in results from the first job and filters the topk results
     *
     * @param key
     * @param value a float value stored as a string
     */
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        float f = Float.parseFloat(value.toString());

        pq.add(new TaxiAndRate(new Text(key), new FloatWritable(f)));
//        String[] values = value.toString().split(",");
//        Integer seconds = Integer.parseInt(values[0]);
//        Float money = Float.parseFloat(values[1]);
//        Float moneyPerMinute = money/(float)seconds/60;

//        pq.add(new TaxiAndRate(new Text(key), new FloatWritable(moneyPerMinute)) );

        if (pq.size() > 10) {
            pq.poll();
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {


        while (pq.size() > 0) {
            TaxiAndRate taxiAndRate = pq.poll();
            context.write(taxiAndRate.getDriver(), taxiAndRate.getRate());
            logger.info("TopKMapper PQ Status: " + pq.toString());
        }
    }

}