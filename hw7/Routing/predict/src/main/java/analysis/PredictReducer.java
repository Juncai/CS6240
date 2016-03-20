package analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.ConnectionInfo;
import utils.DataPreprocessor;
import utils.OTPConsts;
import utils.SimpleFlightInfo;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

// Authors: Jun Cai and Vikas Boddu
public class PredictReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void setup(Context ctx) {
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] possibleKeys = null;
        Map<Integer, ConnectionInfo> acMap = new HashMap<Integer, ConnectionInfo>(); // key: airportID, value: connection info
        SimpleFlightInfo flight;

        for (Text v : values) {
            flight = new SimpleFlightInfo(v);
            if (possibleKeys == null) {
                possibleKeys = ConnectionInfo.generatePossibleKeys(flight.year);
            }
            DataPreprocessor.updateConnectionInfo(acMap, flight);
        }

        ConnectionInfo cci;
        String cKey;
        String[] pKeys;
        int[] durations;
        // calculate the expected duration of each connection
        // then write to the context
        for (Integer ap : acMap.keySet()) {
            cci = acMap.get(ap);
            cci.prepareConnectionMaps();
            // get each connection
            for (int i = 0; i < possibleKeys.length; i++) {
                cKey = possibleKeys[i];
                if (cci.arrMap.containsKey(cKey)) {
                    pKeys = ConnectionInfo.possibleConKeys(possibleKeys, i);
                    for (String k : pKeys) {
                        if (cci.depMap.containsKey(k)) {
                            for (SimpleFlightInfo arr : cci.arrMap.get(cKey)) {
                                for (SimpleFlightInfo dep : cci.depMap.get(k)) {
                                    if (ConnectionInfo.isConnection(arr, dep)) {
                                        // calculate the expected duration
                                        durations = expectedDuration(arr, dep);
                                        // write to context
                                        // P,year,month,day,origin,dest,flNum1,flNum2
                                        // expDuration, duration
                                        context.write(new Text(outputKeyFormatter(arr, dep)),
                                                new Text(durations[0] + OTPConsts.COMMA + durations[1]));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private String outputKeyFormatter(SimpleFlightInfo arr, SimpleFlightInfo dep) {
        return "P" + OTPConsts.COMMA
                + arr.year + OTPConsts.COMMA
                + arr.month + OTPConsts.COMMA
                + arr.day + OTPConsts.COMMA
                + arr.origin + OTPConsts.COMMA
                + dep.dest + OTPConsts.COMMA
                + arr.flightNumber + OTPConsts.COMMA
                + dep.flightNumber;
    }

    private int[] expectedDuration(SimpleFlightInfo arr, SimpleFlightInfo dep) {
        int[] res = new int[2];
        int duration = arr.crsElapsedTime + dep.crsElapsedTime + (int)((dep.crsDepTimeMS - arr.crsArrTimeMS) / 1000 / 60);
        res[0] = duration;
        res[1] = duration;
        if (arr.isDelayed) {
            res[0] += 100 * 60;
        }
        return res;
    }
}
