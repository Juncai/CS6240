package utils;

import org.apache.hadoop.io.Text;

// Authors: Jun Cai and Vikas Boddu
public class SimpleFlightInfo {
    public int flightNumber;
//    public String flightDateStr;
    public int year;
    public int month;
    public int day;
    public int originAirportId;
    public String origin;
    public int destAirportId;
    public String dest;
    public long crsDepTimeMS;
    public long crsArrTimeMS;
    public int crsElapsedTime;
    public boolean isDelayed;

    public SimpleFlightInfo(Text value) {
        String[] values = value.toString().split("_");
        year = Integer.parseInt(values[0]);
        month = Integer.parseInt(values[1]);
        day = Integer.parseInt(values[2]);
        crsDepTimeMS = Long.parseLong(values[3]);
        crsArrTimeMS = Long.parseLong(values[4]);
        originAirportId = Integer.parseInt(values[5]);
        origin = values[6];
        destAirportId = Integer.parseInt(values[7]);
        dest = values[8];
        flightNumber = Integer.parseInt(values[9]);
        crsElapsedTime = Integer.parseInt(values[10]);
        isDelayed = (values[11].equals("1"));
    }
}




