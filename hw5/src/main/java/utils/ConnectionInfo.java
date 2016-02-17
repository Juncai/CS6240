package utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.util.*;

// Authors: Jun Cai and Vikas Boddu
public class ConnectionInfo {
    static private DateTimeFormatter sf = DateTimeFormat.forPattern(OTPConsts.DATEKEY_FORMAT);

    private List<DateTime[]> arrTSs;
    private List<DateTime[]> depTSs;
    private String[] possibleKeys;

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
        arrTSs = new ArrayList<DateTime[]>();
        depTSs = new ArrayList<DateTime[]>();
        possibleKeys = generatePossibleKeys(year);
    }

    public void updateArr(DateTime scheduled, DateTime actual) {
        DateTime[] newEntry = {scheduled, actual};
        arrTSs.add(newEntry);
    }

    public void updateDep(DateTime scheduled, DateTime actual) {
        DateTime[] newEntry = {scheduled, actual};
        depTSs.add(newEntry);
    }

    public int countMissedConnections() {

        Map<String, List<DateTime[]>> depMap = new HashMap<String, List<DateTime[]>>();
        Map<String, List<DateTime[]>> arrMap = new HashMap<String, List<DateTime[]>>();

        fillConMap(depMap, depTSs);
        fillConMap(arrMap, arrTSs);

        return countMissedConnectionsHelper(depMap, arrMap);
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

    private int missedConBetweenLOD(List<DateTime[]> depLod, List<DateTime[]> arrLod) {
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
