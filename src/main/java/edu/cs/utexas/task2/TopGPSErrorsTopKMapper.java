package edu.cs.utexas.task2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class TopGPSErrorsTopKMapper extends Mapper<Text, Text, Text, FloatWritable> {

    private Logger logger = Logger.getLogger(TopGPSErrorsTopKMapper.class);


    private PriorityQueue<IdAndError> pq;

    public void setup(Context context) {
        pq = new PriorityQueue<>();

    }

    /**
     * Reads in results from the first job and filters the topk results
     *
     * @param key
     * @param value a float value stored as a string
     */
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {


        float count = Float.parseFloat(value.toString());

        pq.add(new IdAndError(new Text(key), new FloatWritable(count)) );

        if (pq.size() > 5) {
            pq.poll();
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {


        while (!pq.isEmpty()) {
            IdAndError wordAndCount = pq.poll();
            context.write(wordAndCount.getId(), wordAndCount.getError());
            logger.info("TopGPSErrorsTopKMapper PQ Status: " + pq.toString());
        }
    }

}