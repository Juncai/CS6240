package utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Authors: Jun Cai and Vikas Boddu
public class ConnectionInfo {
    static private DateTimeFormatter sf = DateTimeFormat.forPattern(OTPConsts.DATEKEY_FORMAT);

    private Map<String, List<SimpleFlightInfo>> depMap;
    private Map<String, List<SimpleFlightInfo>> arrMap;
    private String[] possibleKeys;
    // flightNum, origin/dest, arr/dep TS, elapsedTime
    List<SimpleFlightInfo> arrFlights;
    List<SimpleFlightInfo> depFlights;

    static private String[] generatePossibleKeys(int year) {

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
        arrFlights = new ArrayList<long[]>();
        depFlights = new ArrayList<long[]>();
        possibleKeys = generatePossibleKeys(year);
    }

    public void updateArr(SimpleFlightInfo f) {
        long[] newEntry = {(long)f.flightNumber, (long)f.origin, f.crsArrTimeMS, (long)f.crsElapsedTime};
        arrFlights.add(newEntry);
    }

    public void updateDep(SimpleFlightInfo f) {
        long[] newEntry = {(long)f.flightNumber, (long)f.dest, f.crsDepTimeMS, (long)f.crsElapsedTime};
        depFlights.add(newEntry);
    }

    public int countMissedConnections() {

        depMap = new HashMap<String, List<DateTime[]>>();
        arrMap = new HashMap<String, List<DateTime[]>>();

        fillConMap(depMap, depFlights);
        fillConMap(arrMap, arrFlights);

        return countMissedConnectionsHelper(depMap, arrMap);
    }

    public List<DateTime[]> firstDepartures() {
        List<DateTime[]> res = new ArrayList<DateTime[]>();
        for (int i = 0; i < 6; i++) {
            if (depMap.containsKey(possibleKeys[i])) {
                res.addAll(depMap.get(possibleKeys[i]));
            }
        }
        return res;
    }

    public List<DateTime[]> lastArrivals() {
        List<DateTime[]> res = new ArrayList<DateTime[]>();
        int len = possibleKeys.length;
        for (int i = len - 6; i < len; i++) {
            if (arrMap.containsKey(possibleKeys[i])) {
                res.addAll(arrMap.get(possibleKeys[i]));
            }
        }
        return res;
    }

    public int countMissedConnectionsHelper(Map<String, List<DateTime[]>> depMap,
                                            Map<String, List<DateTime[]>> arrMap) {
        int res = 0;
        String cKey;
        String[] pKeys;
        for (int i = 0; i < possibleKeys.length; i++) {
            cKey = possibleKeys[i];
            if (arrMap.containsKey(cKey)) {
                pKeys = possibleConKeys(i);
                for (String k : pKeys) {
                    if (depMap.containsKey(k)) {
                        res += missedConBetweenLOD(depMap.get(k), arrMap.get(cKey));
                    }
                }
            }
        }
        return res;
    }

    int missedConBetweenLOD(List<DateTime[]> depLod, List<DateTime[]> arrLod) {
        int res = 0;
        for (DateTime[] arr : arrLod) {
            for (DateTime[] dep : depLod) {
                res += isMissedConnection(dep, arr);
            }
        }
        return res;
    }

    private int isMissedConnection(DateTime[] dep, DateTime[] arr) {
        // check if it's a valid connection
        if (arr[0].plusMinutes(29).isBefore(dep[0])
                && arr[0].plusMinutes(361).isAfter(dep[0])) {
            if (arr[1].plusMinutes(30).isAfter(dep[1])) {
                return 1;
            }
        }
        return 0;
    }

    private String[] possibleConKeys(int ind) {
        // since the key set is deterministic, it can be pre-computed!!
        int numOfKeys = (possibleKeys.length - ind < 7) ? (possibleKeys.length - ind) : 7;
        String[] res = new String[numOfKeys];

        for (int i = 0; i < res.length; i++) {
            res[i] = possibleKeys[ind + i];
        }
        return res;
    }

    private void fillConMap(Map<String, List<DateTime[]>> map, List<DateTime[]> lod) {
        for (DateTime[] tss :
                lod) {
            DateTime scheduled = tss[0];
            String dateKey = dateToKey(scheduled);
            if (!map.containsKey(dateKey)) {
                map.put(dateKey, new ArrayList<DateTime[]>());
            }
            map.get(dateKey).add(tss);
        }
    }

    private String dateToKey(DateTime d) {
        return d.toString(sf);
    }
}
