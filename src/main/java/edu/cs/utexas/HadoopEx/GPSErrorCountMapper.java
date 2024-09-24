package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.time.format.DateTimeParseException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.time.LocalDateTime;

public class GPSErrorCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		boolean PICKUP_ERROR_FLAG = false;
		boolean DROPOFF_ERROR_FLAG = false;

		// Check if line is valid
		String[] tokens = value.toString().split(",");
		if (tokens.length != Constants.EXPECTED_TOKENS) {
			return;
		}

		// Extract pickup and dropoff datetimes
		String pickupDateTimeString = tokens[Constants.PICKUP_DATETIME_INDEX];
		int pickupHour;
		try {
			LocalDateTime pickupDateTime = LocalDateTime.parse(pickupDateTimeString, Constants.formatter);
			pickupHour = pickupDateTime.getHour();
		} catch (DateTimeParseException e) {
			return;
		}

		//Since we are doing 1-24 index base, 0 represents 24 here
		if(pickupHour==0){
			pickupHour=24;
		}

		String dropoffDateTimeString = tokens[Constants.DROPOFF_DATETIME_INDEX];
		int dropoffHour;
		try {
			LocalDateTime dropoffDateTime = LocalDateTime.parse(dropoffDateTimeString, Constants.formatter);
			dropoffHour = dropoffDateTime.getHour();
		} catch (NumberFormatException e) {
			return;
		}

		if(dropoffHour==0){
			dropoffHour=24;
		}

		// Extract pickup and dropoff coordinates
		String pickupLongitudeString = tokens[Constants.PICKUP_LONGITUDE_INDEX];
		String pickupLatitudeString = tokens[Constants.PICKUP_LATITUDE_INDEX];
		if (pickupLongitudeString.isEmpty() || pickupLatitudeString.isEmpty()) {
			pickupLongitudeString = "0";
			pickupLatitudeString = "0";
		}

		float pickupLongitude;
		float pickupLatitude;
		try {
			pickupLongitude = Float.parseFloat(pickupLongitudeString);
			pickupLatitude = Float.parseFloat(pickupLatitudeString);
		} catch (NumberFormatException e) {
			return;
		}
		if (pickupLongitude == 0 || pickupLatitude == 0) {
			PICKUP_ERROR_FLAG = true;
		}


		String dropoffLongitudeString = tokens[Constants.DROPOFF_LONGITUDE_INDEX];
		String dropoffLatitudeString = tokens[Constants.DROPOFF_LATITUDE_INDEX];
		if (dropoffLongitudeString.isEmpty() || dropoffLatitudeString.isEmpty()) {
			dropoffLongitudeString = "0";
			dropoffLatitudeString = "0";
		}

		float dropoffLongitude;
		float dropoffLatitude;
		try {
			dropoffLongitude = Float.parseFloat(dropoffLongitudeString);
			dropoffLatitude = Float.parseFloat(dropoffLatitudeString);
		} catch (NumberFormatException e) {
			return;
		}
		if (dropoffLongitude == 0 || dropoffLatitude == 0) {
			DROPOFF_ERROR_FLAG = true;
		}


		if (PICKUP_ERROR_FLAG) {
			context.write(new IntWritable(pickupHour), new IntWritable(1));
		}
		if (DROPOFF_ERROR_FLAG) {
			context.write(new IntWritable(dropoffHour), new IntWritable(1));
		}

	}
}