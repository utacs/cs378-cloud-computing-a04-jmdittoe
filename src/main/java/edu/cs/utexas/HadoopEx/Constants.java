package edu.cs.utexas.HadoopEx;

import java.time.format.DateTimeFormatter;

public class Constants {
    // Expected number of tokens in a taxi string
    public final static int EXPECTED_TOKENS = 17;
    // Given a datetime string, the index at which HOUR begins
    public final static int DATETIME_HOUR_INDEX = 14;

    // Date format
    public final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Pickup related constants
    public final static int PICKUP_DATETIME_INDEX = 2;
    public final static int PICKUP_LONGITUDE_INDEX = 6;
    public final static int PICKUP_LATITUDE_INDEX = 7;

    // Dropoff related constants
    public final static int DROPOFF_DATETIME_INDEX = 3;
    public final static int DROPOFF_LONGITUDE_INDEX = 8;
    public final static int DROPOFF_LATITUDE_INDEX = 9;
}
