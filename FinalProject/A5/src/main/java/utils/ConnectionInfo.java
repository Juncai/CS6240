package job.assignment5.utils;

import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.util.*;

// Authors: Jun Cai and Vikas Boddu
public class ConnectionInfo {
    static private DateTimeFormatter sf = DateTimeFormat.forPattern(OTPConsts.DATEKEY_FORMAT);

    private Map<String, List<LocalDateTime[]>> depMap;
    private Map<String, List<LocalDateTime[]>> arrMap;
    private String[] possibleKeys;
    List<LocalDateTime[]> arrTSs;
    List<LocalDateTime[]> depTSs;

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

    public ConnectionInfo() {
        arrTSs = new ArrayList<LocalDateTime[]>();
        depTSs = new ArrayList<LocalDateTime[]>();
    }

    public ConnectionInfo(int year) {
        arrTSs = new ArrayList<LocalDateTime[]>();
        depTSs = new ArrayList<LocalDateTime[]>();
        possibleKeys = generatePossibleKeys(year);
    }

    public void updateArr(LocalDateTime scheduled, LocalDateTime actual) {
        LocalDateTime[] newEntry = {scheduled, actual};
        arrTSs.add(newEntry);
    }

    public void updateDep(LocalDateTime scheduled, LocalDateTime actual) {
        LocalDateTime[] newEntry = {scheduled, actual};
        depTSs.add(newEntry);
    }

    public int countMissedConnections() {

        depMap = new HashMap<String, List<LocalDateTime[]>>();
        arrMap = new HashMap<String, List<LocalDateTime[]>>();

        fillConMap(depMap, depTSs);
        fillConMap(arrMap, arrTSs);

        return countMissedConnectionsHelper(depMap, arrMap);
    }

    public List<LocalDateTime[]> firstDepartures() {
        List<LocalDateTime[]> res = new ArrayList<LocalDateTime[]>();
        for (int i = 0; i < 6; i++) {
            if (depMap.containsKey(possibleKeys[i])) {
                res.addAll(depMap.get(possibleKeys[i]));
            }
        }
        return res;
    }

    public List<LocalDateTime[]> lastArrivals() {
        List<LocalDateTime[]> res = new ArrayList<LocalDateTime[]>();
        int len = possibleKeys.length;
        for (int i = len - 6; i < len; i++) {
            if (arrMap.containsKey(possibleKeys[i])) {
                res.addAll(arrMap.get(possibleKeys[i]));
            }
        }
        return res;
    }

    public int countMissedConnectionsHelper(Map<String, List<LocalDateTime[]>> depMap,
                                            Map<String, List<LocalDateTime[]>> arrMap) {
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

    int missedConBetweenLOD(List<LocalDateTime[]> depLod, List<LocalDateTime[]> arrLod) {
        int res = 0;
        for (LocalDateTime[] arr : arrLod) {
            for (LocalDateTime[] dep : depLod) {
                res += isMissedConnection(dep, arr);
            }
        }
        return res;
    }

    private int isMissedConnection(LocalDateTime[] dep, LocalDateTime[] arr) {
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

    private void fillConMap(Map<String, List<LocalDateTime[]>> map, List<LocalDateTime[]> lod) {
        for (LocalDateTime[] tss :
                lod) {
            LocalDateTime scheduled = tss[0];
            String dateKey = dateToKey(scheduled);
            if (!map.containsKey(dateKey)) {
                map.put(dateKey, new ArrayList<LocalDateTime[]>());
            }
            map.get(dateKey).add(tss);
        }
    }

    private String dateToKey(LocalDateTime d) {
        return d.toString(sf);
    }
}
