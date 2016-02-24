import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

/*
 * Authors: Vedant Naik, Rohan Joshi
 * */
public class FileRecord {
	class Field {
		static final String CRS_ARR_TIME = "CRS_ARR_TIME";
		static final String CRS_DEP_TIME = "CRS_DEP_TIME";
		static final String CRS_ELAPSED_TIME = "CRS_ELAPSED_TIME";

		static final String ORIGIN_AIRPORT_ID = "ORIGIN_AIRPORT_ID";
		static final String ORIGIN_AIRPORT_SEQ_ID = "ORIGIN_AIRPORT_SEQ_ID";
		static final String ORIGIN_CITY_MARKET_ID = "ORIGIN_CITY_MARKET_ID";
		static final String ORIGIN_STATE_FIPS = "ORIGIN_STATE_FIPS";
		static final String ORIGIN_WAC = "ORIGIN_WAC";
		static final String DEST_AIRPORT_ID = "DEST_AIRPORT_ID";
		static final String DEST_AIRPORT_SEQ_ID = "DEST_AIRPORT_SEQ_ID";
		static final String DEST_CITY_MARKET_ID = "DEST_CITY_MARKET_ID";
		static final String DEST_STATE_FIPS = "DEST_STATE_FIPS";
		static final String DEST_WAC = "DEST_WAC";

		static final String ORIGIN = "ORIGIN";
		static final String ORIGIN_CITY_NAME = "ORIGIN_CITY_NAME";
		static final String ORIGIN_STATE_ABR = "ORIGIN_STATE_ABR";
		static final String ORIGIN_STATE_NM = "ORIGIN_STATE_NM";
		static final String DEST = "DEST";
		static final String DEST_CITY_NAME = "DEST_CITY_NAME";
		static final String DEST_STATE_ABR = "DEST_STATE_ABR";
		static final String DEST_STATE_NM = "DEST_STATE_NM";

		static final String CANCELLED = "CANCELLED";

		static final String ARR_TIME = "ARR_TIME";
		static final String DEP_TIME = "DEP_TIME";
		static final String ACTUAL_ELAPSED_TIME = "ACTUAL_ELAPSED_TIME";

		static final String ARR_DELAY = "ARR_DELAY";
		static final String ARR_DELAY_NEW = "ARR_DELAY_NEW";
		static final String ARR_DEL15 = "ARR_DEL15";

		static final String AVG_TICKET_PRICE = "AVG_TICKET_PRICE";

		static final String FL_DATE = "FL_DATE";
		static final String CARRIER = "CARRIER";

		static final String YEAR = "YEAR";
		static final String MONTH = "MONTH";
		
		static final String DISTANCE = "DISTANCE";
		static final String AIR_TIME = "AIR_TIME";
		
		static final String DIV_DISTANCE = "DIV_DISTANCE";
	}

	public static final String[] csvh = { "YEAR", "QUARTER", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK", "FL_DATE",
			"UNIQUE_CARRIER", "AIRLINE_ID", "CARRIER", "TAIL_NUM", "FL_NUM", "ORIGIN_AIRPORT_ID",
			"ORIGIN_AIRPORT_SEQ_ID", "ORIGIN_CITY_MARKET_ID", "ORIGIN", "ORIGIN_CITY_NAME", "ORIGIN_STATE_ABR",
			"ORIGIN_STATE_FIPS", "ORIGIN_STATE_NM", "ORIGIN_WAC", "DEST_AIRPORT_ID", "DEST_AIRPORT_SEQ_ID",
			"DEST_CITY_MARKET_ID", "DEST", "DEST_CITY_NAME", "DEST_STATE_ABR", "DEST_STATE_FIPS", "DEST_STATE_NM",
			"DEST_WAC", "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "DEP_DELAY_NEW", "DEP_DEL15", "DEP_DELAY_GROUP",
			"DEP_TIME_BLK", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY",
			"ARR_DELAY_NEW", "ARR_DEL15", "ARR_DELAY_GROUP", "ARR_TIME_BLK", "CANCELLED", "CANCELLATION_CODE",
			"DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "FLIGHTS", "DISTANCE", "DISTANCE_GROUP",
			"CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY", "FIRST_DEP_TIME",
			"TOTAL_ADD_GTIME", "LONGEST_ADD_GTIME", "DIV_AIRPORT_LANDINGS", "DIV_REACHED_DEST",
			"DIV_ACTUAL_ELAPSED_TIME", "DIV_ARR_DELAY", "DIV_DISTANCE", "DIV1_AIRPORT", "DIV1_AIRPORT_ID",
			"DIV1_AIRPORT_SEQ_ID", "DIV1_WHEELS_ON", "DIV1_TOTAL_GTIME", "DIV1_LONGEST_GTIME", "DIV1_WHEELS_OFF",
			"DIV1_TAIL_NUM", "DIV2_AIRPORT", "DIV2_AIRPORT_ID", "DIV2_AIRPORT_SEQ_ID", "DIV2_WHEELS_ON",
			"DIV2_TOTAL_GTIME", "DIV2_LONGEST_GTIME", "DIV2_WHEELS_OFF", "DIV2_TAIL_NUM", "DIV3_AIRPORT",
			"DIV3_AIRPORT_ID", "DIV3_AIRPORT_SEQ_ID", "DIV3_WHEELS_ON", "DIV3_TOTAL_GTIME", "DIV3_LONGEST_GTIME",
			"DIV3_WHEELS_OFF", "DIV3_TAIL_NUM", "DIV4_AIRPORT", "DIV4_AIRPORT_ID", "DIV4_AIRPORT_SEQ_ID",
			"DIV4_WHEELS_ON", "DIV4_TOTAL_GTIME", "DIV4_LONGEST_GTIME", "DIV4_WHEELS_OFF", "DIV4_TAIL_NUM",
			"DIV5_AIRPORT", "DIV5_AIRPORT_ID", "DIV5_AIRPORT_SEQ_ID", "DIV5_WHEELS_ON", "DIV5_TOTAL_GTIME",
			"DIV5_LONGEST_GTIME", "DIV5_WHEELS_OFF", "DIV5_TAIL_NUM", "AVG_TICKET_PRICE" };
	public static final ArrayList<String> csvHeaders = new ArrayList<String>(Arrays.asList(csvh));
	
	protected static String getValueOf(String[] fields, String headerName) {
		return fields[csvHeaders.indexOf(headerName)];
	}

	protected static boolean isRecordValid(String[] fields) {

		// Special quick checks for MissingConnections assignment
		if (fields[csvHeaders.indexOf(Field.CRS_ARR_TIME)].equals("") ||
			fields[csvHeaders.indexOf(Field.CRS_DEP_TIME)].equals("") ||
			fields[csvHeaders.indexOf(Field.ARR_TIME)].equals("") ||
			fields[csvHeaders.indexOf(Field.DEP_TIME)].equals("")){
			return false;
		}
		
		
		float timeZone = 0;

		// CRSArrTime and CRSDepTime should not be zero
		// timeZone % 60 should be 0

		SimpleDateFormat format = new SimpleDateFormat("HHmm");

		String crsArrTime = fields[csvHeaders.indexOf(Field.CRS_ARR_TIME)];
		String crsDepTime = fields[csvHeaders.indexOf(Field.CRS_DEP_TIME)];
		String crsElapsedTime = fields[csvHeaders.indexOf(Field.CRS_ELAPSED_TIME)];

		try {
			Date CRSArrTime = (crsArrTime.equals("") ? null : format.parse(crsArrTime));
			Date CRSDepTime = (crsDepTime.equals("") ? null : format.parse(crsDepTime));
			float CRSElapsedTime = Float.parseFloat(crsElapsedTime);

			float crsDiff = hhmmDiff(crsArrTime, crsDepTime);

			timeZone = crsDiff - CRSElapsedTime;

			if (CRSArrTime.getTime() == 0.0 || CRSDepTime.getTime() == 0.0)
				return false;
			if ((timeZone % 60) != 0)
				return false;

		} catch (NumberFormatException e) {
			return false;
		} catch (ParseException e) {
			return false;
		}

		// AirportID, AirportSeqID, CityMarketID, StateFips, Wac should be
		// larger than 0

		if (Integer.parseInt(fields[csvHeaders.indexOf(Field.ORIGIN_AIRPORT_ID)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeaders.indexOf(Field.ORIGIN_AIRPORT_SEQ_ID)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeaders.indexOf(Field.ORIGIN_CITY_MARKET_ID)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeaders.indexOf(Field.ORIGIN_STATE_FIPS)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeaders.indexOf(Field.ORIGIN_WAC)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeaders.indexOf(Field.DEST_AIRPORT_ID)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeaders.indexOf(Field.DEST_AIRPORT_SEQ_ID)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeaders.indexOf(Field.DEST_CITY_MARKET_ID)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeaders.indexOf(Field.DEST_STATE_FIPS)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeaders.indexOf(Field.DEST_WAC)]) <= 0)
			return false;

		// Origin, Destination, CityName, State, StateName should not be empty

		if (fields[csvHeaders.indexOf(Field.ORIGIN)].equals(""))
			return false;

		if (fields[csvHeaders.indexOf(Field.ORIGIN_CITY_NAME)].equals(""))
			return false;

		if (fields[csvHeaders.indexOf(Field.ORIGIN_STATE_ABR)].equals(""))
			return false;

		if (fields[csvHeaders.indexOf(Field.ORIGIN_STATE_NM)].equals(""))
			return false;

		if (fields[csvHeaders.indexOf(Field.DEST)].equals(""))
			return false;

		if (fields[csvHeaders.indexOf(Field.DEST_CITY_NAME)].equals(""))
			return false;

		if (fields[csvHeaders.indexOf(Field.DEST_STATE_ABR)].equals(""))
			return false;

		if (fields[csvHeaders.indexOf(Field.DEST_STATE_NM)].equals(""))
			return false;

		// For flights that are not Cancelled:
		int cancelledDigit = 0;
		try {
			cancelledDigit = Integer.parseInt(fields[csvHeaders.indexOf(Field.CANCELLED)]);
		} catch (NumberFormatException e) {
			cancelledDigit = 0;
		}

		if (cancelledDigit != 1) {
			String arrTime = fields[csvHeaders.indexOf(Field.ARR_TIME)];
			String depTime = fields[csvHeaders.indexOf(Field.DEP_TIME)];
			String actElapsedTime = fields[csvHeaders.indexOf(Field.ACTUAL_ELAPSED_TIME)];

			try {
				long actualElapsedTime = Long.parseLong(actElapsedTime);
				long actualDiff = hhmmDiff(arrTime, depTime);

				long actualTimeZone = actualDiff - actualElapsedTime;

				long CRSElapsedTime = Long.parseLong(crsElapsedTime);
				long crsDiff = hhmmDiff(crsArrTime, crsDepTime);
				long newtimeZone = crsDiff - CRSElapsedTime;

				if (actualTimeZone != newtimeZone) {
					return false;
				}

				// if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
				// if ArrDelay < 0 then ArrDelayMinutes should be zero
				// if ArrDelayMinutes >= 15 then ArrDel15 should be false
				float arrDelay = Float.parseFloat(fields[csvHeaders.indexOf(Field.ARR_DELAY)]);

				float ArrDelayMinutes = Float.parseFloat(fields[csvHeaders.indexOf(Field.ARR_DELAY_NEW)]);

				float arrDel15 = Float.parseFloat(fields[csvHeaders.indexOf(Field.ARR_DEL15)]);

				if (arrDelay > 0.0) {
					if (arrDelay != ArrDelayMinutes) {
						return false;
					}
				}

				if (arrDelay < 0.0) {
					if (ArrDelayMinutes != 0) {
						return false;
					}
				}

				if (ArrDelayMinutes > 15.0) {
					if (arrDel15 != 1) {
						return false;
					}
				}

			} catch (NumberFormatException e) {
				// no entry found in record
				// format does not match
				return false;
			}
		}
		// Given sanity checks complete

		// Additional validations
		if (Float.parseFloat(fields[csvHeaders.indexOf(Field.AVG_TICKET_PRICE)]) > 999999)
			return false;

		return true;
	}

	public static String makeCompleteHHMM(String hhmm){
		if (hhmm.length() == 3){
			hhmm = "0"+hhmm;
		}
		
		if (hhmm.length() == 2){
			hhmm = "00"+hhmm;
		}
		
		if (hhmm.length() == 1){
			hhmm = "000"+hhmm;
		}
		
		if (hhmm.length() == 0){
			hhmm = "0000";
		}
		
		return hhmm;
	}
	
	private static int hhmmDiff(String arr, String dep) {
		
		arr = makeCompleteHHMM(arr);
		dep = makeCompleteHHMM(dep);
		
		int arrHH = Integer.parseInt(arr.substring(0, 2));
		int arrMM = Integer.parseInt(arr.substring(2, 4));

		int depHH = Integer.parseInt(dep.substring(0, 2));
		int depMM = Integer.parseInt(dep.substring(2, 4));

		if (Integer.parseInt(arr) > Integer.parseInt(dep)) {
			return (arrHH - depHH) * 60 + (arrMM - depMM);
		} else {
			// Cross over 24hr
			return (arrHH - depHH + 24) * 60 + (arrMM - depMM);
		}
	}
}