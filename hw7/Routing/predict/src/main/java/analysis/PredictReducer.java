package analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.ConnectionInfo;
import utils.DataPreprocessor;
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
//        String carrier = key.toString();
//        int year = Integer.parseInt(key.toString().split(",")[1]);

        for (Text v : values) {
            flight = new SimpleFlightInfo(v);
            if (possibleKeys == null) {
                possibleKeys = ConnectionInfo.generatePossibleKeys(flight.getYear());
            }
            DataPreprocessor.updateConnectionInfo(acMap, flight);
        }

        ConnectionInfo cci;
        String cKey;
        String[] pKeys;
        // TODO calculate the expected duration of each connection
        // TODO then write to the context
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
//                            res += missedConBetweenLOD(depMap.get(k), arrMap.get(cKey));
                            for (SimpleFlightInfo arr : cci.arrMap.get(cKey)) {
                                for (SimpleFlightInfo dep : cci.depMap.get(k)) {
//                                    res += isMissedConnection(dep, arr);
                                    if (ConnectionInfo.isConnection(arr, dep)) {
                                        // calculate the expected duration
                                        int duration = expectedDuration(arr, dep);
                                        // write to context
                                        context.write(new Text(), new Text());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

    }

    private int expectedDuration(SimpleFlightInfo arr, SimpleFlightInfo dep) {
        int duration = arr.crsElapsedTime + dep.crsElapsedTime + (int)((dep.crsDepTimeMS - arr.crsArrTimeMS) / 1000 / 60);
        if (arr.isDelayed) {
            // TODO experiment with differnt criterials
            duration += 100 * 60;
        }
        return duration;
    }

//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//    }
}

