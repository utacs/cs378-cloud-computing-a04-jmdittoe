package edu.cs.utexas.task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaxiMapper extends Mapper<Object, Text, Text, TaxiClass> {

	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");
		if (!isValid(tokens)) {
			return;
		}
		try {
			String driver = tokens[1];
			float money = Float.parseFloat(tokens[16]);
			int seconds = Integer.parseInt(tokens[4]);
			word.set(driver);
			TaxiClass t = new TaxiClass(seconds, money);
			context.write(word, t);
		} catch(NumberFormatException ignore) {
		}
	}
	private static boolean isValid(String[] attributes) {
		float val = 0;
		float totalVal;
		int seconds;
		if (attributes.length != 17) {
			// There are 17 commas, if not make note of the error and don't add this to final batch
			return false;
		}
		try {
			for(int i = 11; i < 16; i++) {
				val += Float.parseFloat(attributes[i]);
			}
			totalVal = Float.parseFloat(attributes[16]);
			// Try to parse seconds, no need to store result
			seconds = Integer.parseInt(attributes[4]);
		} catch (NumberFormatException e) {
			// Fare amount not parseable, don't add this to final batch
			return false;
		}
		if(Math.abs(val - totalVal) > 0.0001 || seconds == 0) {
			// The sum of the values is not equal to total,
			// or if time is 0,  don't add this to final batch
			return false;
		}
		// Total value should not exceed 500, if not make note of the error and don't add this to final batch
		return !(totalVal > 500);
	}
}