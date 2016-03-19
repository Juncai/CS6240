package utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Authors: Jun Cai and Vikas Boddu
public class ConnectionInfo {
    static private DateTimeFormatter sf = DateTimeFormat.forPattern(OTPConsts.DATEKEY_FORMAT);

    public Map<String, List<SimpleFlightInfo>> depMap;
    public Map<String, List<SimpleFlightInfo>> arrMap;
//    private String[] possibleKeys;
    // flightNum, origin/dest, arr/dep TS, elapsedTime
    private List<SimpleFlightInfo> arrFlights;
    private List<SimpleFlightInfo> depFlights;

    public static String[] generatePossibleKeys(int year) {

        int daysOfYear = 365;
        String firstHourStr = year + "010100";
        DateTime firstHour = sf.parseDateTime(firstHourStr);
        if (firstHour.year().isLeap()) {
            daysOfYear = 366;
        }
        String[] res = new String[daysOfYear * 24];
        DateTime tmpH = firstHour;
        for (int i = 0; i < res.length; i++) {
            res[i] = tmpH.toString(sf);
            tmpH = tmpH.plusHours(1);
        }

        return res;
    }

    public ConnectionInfo(int year) {
        arrFlights = new ArrayList<SimpleFlightInfo>();
        depFlights = new ArrayList<SimpleFlightInfo>();
//        possibleKeys = generatePossibleKeys(year);
    }

    public void updateArr(SimpleFlightInfo f) {
//        long[] newEntry = {(long)f.flightNumber, (long)f.origin, f.crsArrTimeMS, (long)f.crsElapsedTime};
        arrFlights.add(f);
    }

    public void updateDep(SimpleFlightInfo f) {
//        long[] newEntry = {(long)f.flightNumber, (long)f.dest, f.crsDepTimeMS, (long)f.crsElapsedTime};
        depFlights.add(f);
    }

    public void prepareConnectionMaps() {

        depMap = new HashMap<String, List<SimpleFlightInfo>>();
        arrMap = new HashMap<String, List<SimpleFlightInfo>>();

        fillConMap(depMap, depFlights, false);
        fillConMap(arrMap, arrFlights, true);

//        return countMissedConnectionsHelper(depMap, arrMap);
    }

    public static boolean isConnection(SimpleFlightInfo arr, SimpleFlightInfo dep) {
        // check if it's a valid connection
        if (arr.crsArrTimeMS + 30 * 60 * 1000 <= dep.crsDepTimeMS &&
                arr.crsArrTimeMS + 60 * 60 * 1000 >= dep.crsDepTimeMS) {
            return true;
        }
//        if (arr[0].plusMinutes(29).isBefore(dep[0])
//                && arr[0].plusMinutes(61).isAfter(dep[0])) {
//            return 1;
//        }
        return false;
    }

    public static String[] possibleConKeys(String[] possibleKeys, int ind) {
        // since the key set is deterministic, it can be pre-computed!!
        int numOfKeys = (possibleKeys.length - ind < 2) ? (possibleKeys.length - ind) : 2;
        String[] res = new String[numOfKeys];

        for (int i = 0; i < res.length; i++) {
            res[i] = possibleKeys[ind + i];
        }
        return res;
    }

    private void fillConMap(Map<String, List<SimpleFlightInfo>> map, List<SimpleFlightInfo> lod, boolean isArr) {
        for (SimpleFlightInfo sf : lod) {
            DateTime scheduled = new DateTime(isArr ? sf.crsArrTimeMS : sf.crsDepTimeMS).withZone(DateTimeZone.UTC);
            String dateKey = dateToKey(scheduled);
            if (!map.containsKey(dateKey)) {
                map.put(dateKey, new ArrayList<SimpleFlightInfo>());
            }
            map.get(dateKey).add(sf);
        }
    }

    private String dateToKey(DateTime d) {
        return d.toString(sf);
    }
}
