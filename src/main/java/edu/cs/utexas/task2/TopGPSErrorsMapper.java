package edu.cs.utexas.task2;

import edu.cs.utexas.task1.Constants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

public class TopGPSErrorsMapper extends Mapper<Object, Text, Text, TopGPSErrorsWrapper> {

	private Text taxiID = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		boolean PICKUP_ERROR_FLAG = false;
		boolean DROPOFF_ERROR_FLAG = false;

		// Check if line has right number of attributes
		String[] tokens = value.toString().split(",");
		if (tokens.length != Constants.EXPECTED_TOKENS) {
			return;
		}

		//Check to see if taxdriver is parsable
		String id = tokens[0];
		if(id.isEmpty()){
			return;
		}
		//Set id into a text object
		taxiID.set(id);
		//Set up wrapper. Default is that there is an error
		TopGPSErrorsWrapper wrapper = new TopGPSErrorsWrapper();

		// Check to see if pickup and dropoff hours are parsable
		String pickupDateTimeString = tokens[Constants.PICKUP_DATETIME_INDEX];
		int pickupHour;
		try {
			LocalDateTime pickupDateTime = LocalDateTime.parse(pickupDateTimeString, Constants.formatter);
			pickupHour = pickupDateTime.getHour();
		} catch (DateTimeParseException e) {
			context.write(taxiID, wrapper);
			return;
		}

		String dropoffDateTimeString = tokens[Constants.DROPOFF_DATETIME_INDEX];
		int dropoffHour;
		try {
			LocalDateTime dropoffDateTime = LocalDateTime.parse(dropoffDateTimeString, Constants.formatter);
			dropoffHour = dropoffDateTime.getHour();
		} catch (NumberFormatException e) {
			context.write(taxiID, wrapper);
			return;
		}

		// Extract pickup and dropoff coordinates
		String pickupLongitudeString = tokens[Constants.PICKUP_LONGITUDE_INDEX];
		String pickupLatitudeString = tokens[Constants.PICKUP_LATITUDE_INDEX];
		if (pickupLongitudeString.isEmpty() || pickupLatitudeString.isEmpty()) {
			context.write(taxiID, wrapper);
			return;
		}

		float pickupLongitude;
		float pickupLatitude;
		try {
			pickupLongitude = Float.parseFloat(pickupLongitudeString);
			pickupLatitude = Float.parseFloat(pickupLatitudeString);
		} catch (NumberFormatException e) {
			context.write(taxiID, wrapper);
			return;
		}
		if (pickupLongitude == 0 || pickupLatitude == 0) {
			context.write(taxiID, wrapper);
			return;
		}


		String dropoffLongitudeString = tokens[Constants.DROPOFF_LONGITUDE_INDEX];
		String dropoffLatitudeString = tokens[Constants.DROPOFF_LATITUDE_INDEX];
		if (dropoffLongitudeString.isEmpty() || dropoffLatitudeString.isEmpty()) {
			context.write(taxiID, wrapper);
			return;
		}

		float dropoffLongitude=0;
		float dropoffLatitude=0;
		try {
			dropoffLongitude = Float.parseFloat(dropoffLongitudeString);
			dropoffLatitude = Float.parseFloat(dropoffLatitudeString);
		} catch (NumberFormatException e) {
			context.write(taxiID, wrapper);
			return;
		}
		if (dropoffLongitude == 0 || dropoffLatitude == 0) {
			context.write(taxiID, wrapper);
			return;
		}

		//No errors found, change to have no error and pass on.
		wrapper.changeToNoError();
		context.write(taxiID, wrapper);

	}
}