//YEAR
//QUARTER
//MONTH
//DAY_OF_MONTH
//DAY_OF_WEEK
//FL_DATE  IS_HOLIDAY
//UNIQUE_CARRIER
//-----TAIL_NUM------
//FL_NUM
//ORIGIN_AIRPORT_ID
//ORIGIN_CITY_MARKET_ID
//ORIGIN_STATE_FIPS
//DEST_AIRPORT_ID
//DEST_CITY_MARKET_ID
//DEST_STATE_FIPS
//CRS_DEP_TIME DEP_HOUR_OF_DAY
//CRS_ARR_TIME ARR_HOUR_OF_DAY
//ARR_DELAY
//CRS_ELAPSED_TIME
//----DISTANCE-----
//DISTANCE_GROUP
package utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

// Authors: Jun Cai and Vikas Boddu
public class FlightInfo {
    static private DateTimeFormatter sf = DateTimeFormat.forPattern(OTPConsts.DATETIME_FORMAT).withZone(DateTimeZone.UTC);
    static private DateTimeFormatter ff = DateTimeFormat.forPattern(OTPConsts.FL_DATE_FORMAT).withZone(DateTimeZone.UTC);
    public int quarter;
    public int month;
    public int dayOfMonth;
    public int dayOfWeek;

    public String carrier;
    private String flightNumber;
    public DateTime flightDate;
    public String flightDateStr;
    public int originAirportId;
    public int originCityMarket;
    public int originStateFips;
    public int destAirportId;
    public int destCityMarket;
    public int destStateFips;
    public int distanceGroup;
    public DateTime crsDepTime;
    public String crsDepTimeStr;
    public DateTime crsArrTime;
    public int crsElapsedTime;
    public boolean isDelayed;
    private boolean isValid = true;
    private boolean isTest = false;

    public FlightInfo(String line, boolean isTest) {
        this.isTest = isTest;

        String[] values;
        values = DataPreprocessor.parseCSVLine(line, true);

        // skip sanity check for test data
        if (!isTest) {
            isValid = DataPreprocessor.sanityCheck(values);
        }

        if (isValid) {
            flightDateStr = values[OTPConsts.FL_DATE];
            flightDate = ff.parseDateTime(flightDateStr);
            String date = values[OTPConsts.FL_DATE];
            crsDepTimeStr = values[OTPConsts.CRS_DEP_TIME];
            String arrTimeScheduledStr = values[OTPConsts.CRS_ARR_TIME];

            crsDepTime = getDateTime(date, crsDepTimeStr);
            crsArrTime = getDateTime(date, arrTimeScheduledStr);

            // consider the case when arrive in a new day
            if (crsArrTime.isBefore(crsDepTime)) {
                crsArrTime = crsArrTime.plusDays(1);
            }

            quarter = Integer.parseInt(values[OTPConsts.QUARTER]);
            month = Integer.parseInt(values[OTPConsts.MONTH]);
            dayOfMonth = Integer.parseInt(values[OTPConsts.DAY_OF_MONTH]);
            dayOfWeek = Integer.parseInt(values[OTPConsts.DAY_OF_WEEK]);
            carrier = values[OTPConsts.UNIQUE_CARRIER];
            originAirportId = Integer.parseInt(values[OTPConsts.ORIGIN_AIRPORT_ID]);
            originCityMarket = Integer.parseInt(values[OTPConsts.ORIGIN_CITY_MARKET_ID]);
            originStateFips = Integer.parseInt(values[OTPConsts.ORIGIN_STATE_FIPS]);
            destAirportId = Integer.parseInt(values[OTPConsts.DEST_AIRPORT_ID]);
            destCityMarket = Integer.parseInt(values[OTPConsts.ORIGIN_CITY_MARKET_ID]);
            destStateFips = Integer.parseInt(values[OTPConsts.DEST_STATE_FIPS]);
            crsElapsedTime = Integer.parseInt(values[OTPConsts.CRS_ELAPSED_TIME]);
            distanceGroup = Integer.parseInt(values[OTPConsts.DISTANCE_GROUP]);
            flightNumber = values[OTPConsts.FL_NUM];
            if (!this.isTest) {
                isDelayed = Double.parseDouble(values[OTPConsts.ARR_DELAY]) > 0;
            } else {
                isDelayed = false;
            }
        }
    }

    private DateTime getDateTime(String dateStr, String timeStr) {
        // a hack to replace 2400
        if (timeStr.equals(OTPConsts.START_OF_NEW_DAY_OLD)) {
            timeStr = OTPConsts.START_OF_NEW_DAY;
        }
        return sf.parseDateTime(dateStr + " " + timeStr);
    }

    public boolean isValid() {
        return isValid;
    }

    /*
    quarter, month, dayOfMonth, dayOfWeek, carrier, isHoliday, originAI, originCity, originState,
    destAI, destCity, destState, distanceGroup, depHourOfDay, arrHourOfDay, elapsedTimeInHours, flNum,
     fldate, crsDepTime, isDelay
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(quarter + OTPConsts.COMMA);
        sb.append(month + OTPConsts.COMMA);
        sb.append(dayOfMonth + OTPConsts.COMMA);
        sb.append(dayOfWeek + OTPConsts.COMMA);
        sb.append(carrier + OTPConsts.COMMA);
        sb.append((isHolidy(flightDate) ? 1 : 0) + OTPConsts.COMMA);
        sb.append(originAirportId + OTPConsts.COMMA);
        sb.append(originCityMarket + OTPConsts.COMMA);
        sb.append(originStateFips + OTPConsts.COMMA);
        sb.append(destAirportId + OTPConsts.COMMA);
        sb.append(destCityMarket + OTPConsts.COMMA);
        sb.append(destStateFips + OTPConsts.COMMA);
        sb.append(distanceGroup + OTPConsts.COMMA);
        sb.append(crsDepTime.getHourOfDay() + OTPConsts.COMMA);
        sb.append(crsArrTime.getHourOfDay() + OTPConsts.COMMA);
        sb.append(crsElapsedTime / 60 + OTPConsts.COMMA);
        sb.append(flightNumber + OTPConsts.COMMA);
        sb.append(flightDateStr + OTPConsts.COMMA);
        sb.append(crsDepTimeStr + OTPConsts.COMMA);
        sb.append((isDelayed ? 1 : 0) + "\n");

        return sb.toString();
    }

    private boolean isHolidy(DateTime dt) {
        // Christmas and New Year
        if (month == 12) {
            if ( dayOfMonth >= 18) return true;
        }
        if (month == 1) {
            if ( dayOfMonth <= 8) return true;
        }

        // TODO Thanksgiving
//        int weekOfNov =
//        if (month == 11 && ) {
//
//        }




        return false;
    }
}




