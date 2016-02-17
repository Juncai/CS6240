package utils;

import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * Created by jon on 2/14/16.
 */
public class FlightInfo {
    static private DateTimeFormatter sf = DateTimeFormat.forPattern(OTPConsts.DATETIME_FORMAT);
    private String carrier;
    private int year;
    private int originalAirportId;
    private int destAirportId;
    private DateTime arrTimeActual;
    private DateTime arrTimeScheduled;
    private DateTime depTimeActual;
    private DateTime depTimeScheduled;
    private boolean isValid = true;

    public FlightInfo(Text value) {
        String[] values = value.toString().split(" ");
        originalAirportId = Integer.parseInt(values[0]);
        depTimeScheduled = new DateTime(Long.parseLong(values[1]));
        depTimeActual = new DateTime(Long.parseLong(values[2]));
        destAirportId = Integer.parseInt(values[3]);
        arrTimeScheduled = new DateTime(Long.parseLong(values[4]));
        arrTimeActual = new DateTime(Long.parseLong(values[5]));
    }

    public FlightInfo(String line) {

        String[] values;
        values = DataPreprocessor.parseCSVLine(line);
        if (!DataPreprocessor.sanityCheck(values)) {
            isValid = false;
        } else {
            year = Integer.parseInt(values[OTPConsts.YEAR]);
            String date = values[OTPConsts.FL_DATE];
            String depTimeActualStr = values[OTPConsts.DEP_TIME];
            String arrTimeActualStr = values[OTPConsts.ARR_TIME];
            // a hack to replace 2400
            if (depTimeActualStr.equals(OTPConsts.START_OF_NEW_DAY_OLD)) {
                depTimeActualStr = OTPConsts.START_OF_NEW_DAY;
            }
            if (arrTimeActualStr.equals(OTPConsts.START_OF_NEW_DAY_OLD)) {
                arrTimeActualStr = OTPConsts.START_OF_NEW_DAY;
            }
            int arrDelay = ((Double)Double.parseDouble(values[OTPConsts.ARR_DELAY])).intValue();
            int depDelay = ((Double)Double.parseDouble(values[OTPConsts.DEP_DELAY])).intValue();
            carrier = values[OTPConsts.UNIQUE_CARRIER];
            originalAirportId = Integer.parseInt(values[OTPConsts.ORIGIN_AIRPORT_ID]);
            destAirportId = Integer.parseInt(values[OTPConsts.DEST_AIRPORT_ID]);
            depTimeActual = sf.parseDateTime(date + " " + depTimeActualStr);
            arrTimeActual = sf.parseDateTime(date + " " + arrTimeActualStr);
            // consider the case when arrive in a new day
            if (arrTimeActual.isBefore(depTimeActual)) {
                arrTimeActual = arrTimeActual.plusDays(1);
            }
            arrTimeScheduled = arrTimeActual.minusMinutes(arrDelay);
            depTimeScheduled = depTimeActual.minusMinutes(depDelay);
        }
    }

    public String getCarrier() {
        return carrier;
    }

    public int getOriginalAirportId() {
        return originalAirportId;
    }

    public int getDestAirportId() {
        return destAirportId;
    }

    public int getYear() {
        return year;
    }

    public DateTime getDepTimeActual() {
        return depTimeActual;
    }

    public DateTime getArrTimeActual() {
        return arrTimeActual;
    }

    public DateTime getDepTimeScheduled() {
        return depTimeScheduled;
    }

    public DateTime getArrTimeScheduled() {
        return arrTimeScheduled;
    }

    public boolean isValid() {
        return isValid;
    }
}




