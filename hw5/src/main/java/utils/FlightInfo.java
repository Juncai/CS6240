package utils;

import org.apache.commons.lang.time.DateUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by jon on 2/14/16.
 */
public class FlightInfo {
    private String carrier;
    private int originalAirportId;
    private int destAirportId;
    private Date arrTime;
    private Date depTime;
    private boolean isValid = true;

    public FlightInfo(String line) {

        String[] values;
        values = DataPreprocessor.parseCSVLine(line);
        if (!DataPreprocessor.sanityCheck(values)) {
            isValid = false;
        } else {
            String date = values[OTPConsts.FL_DATE];
            String depTimeStr = values[OTPConsts.DEP_TIME];
            String arrTimeStr = values[OTPConsts.ARR_TIME];
            carrier = values[OTPConsts.UNIQUE_CARRIER];
            originalAirportId = Integer.parseInt(values[OTPConsts.ORIGIN_AIRPORT_ID]);
            destAirportId = Integer.parseInt(values[OTPConsts.DEST_AIRPORT_ID]);
            SimpleDateFormat sf = new SimpleDateFormat(OTPConsts.DATETIME_FORMAT);
            try {
                depTime = sf.parse(date + " " + depTimeStr);
                arrTime = sf.parse(date + " " + arrTimeStr);
                // consider the case when arrive in a new day
                if (arrTime.getTime() - depTime.getTime() < 0) {
                    arrTime = DateUtils.addDays(arrTime, 1);
                }
            } catch (ParseException ex) {
                System.out.print(ex);
            }
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

    public Date getDepTime() {
        return depTime;
    }

    public Date getArrTime() {
        return arrTime;
    }

    public boolean isValid() {
        return isValid;
    }
}




