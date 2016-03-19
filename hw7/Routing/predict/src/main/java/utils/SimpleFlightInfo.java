package utils;

import org.apache.hadoop.io.Text;

// Authors: Jun Cai and Vikas Boddu
public class SimpleFlightInfo {
    public int flightNumber;
    public String flightDateStr;
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
        flightDateStr = values[0];
        crsDepTimeMS = Long.parseLong(values[1]);
        crsArrTimeMS = Long.parseLong(values[2]);
        originAirportId = Integer.parseInt(values[3]);
        origin = values[4];
        destAirportId = Integer.parseInt(values[5]);
        dest = values[6];
        flightNumber = Integer.parseInt(values[7]);
        crsElapsedTime = Integer.parseInt(values[8]);
        isDelayed = (values[9].equals("1"));
    }

    public int getYear() {
        return Integer.parseInt(flightDateStr.substring(0, 4));
    }
}




