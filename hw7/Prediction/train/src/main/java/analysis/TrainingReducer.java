package analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// Authors: Jun Cai and Vikas Boddu
public class TrainingReducer extends Reducer<Text, Text, Text, IntWritable> {
    // (Carrier, (Year, connections across different years))
    // here the missed cross year connections will be counted in the previous year's stats.
    static private Map<String, Map<Integer, CrossYearConnectionInfo>> crossYearMap;
    static private Map<String, Map<Integer, Integer>> missedConMap;

    @Override
    protected void setup(Context ctx) {
        crossYearMap = new HashMap<String, Map<Integer, CrossYearConnectionInfo>>();
        missedConMap = new HashMap<String, Map<Integer, Integer>>();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        Map<Integer, ConnectionInfo> acMap = new HashMap<Integer, ConnectionInfo>(); // key: airportID, value: connection info
        FlightInfo flight;
        String carrier = key.toString().split(",")[0];
        int year = Integer.parseInt(key.toString().split(",")[1]);

        for (Text v : values) {
            flight = new FlightInfo(v);
            DataPreprocessor.updateConnectionInfo(acMap, flight, year);
        }

        int missedCount = 0;
        ConnectionInfo cci;
        for (Integer ap : acMap.keySet()) {
            cci = acMap.get(ap);
            missedCount += cci.countMissedConnections();

            // add first departures and last arrivals to the cross year map
            updateCrossYearMap(carrier, year, cci);
        }

        // update the missed connection map
        updateMissedConMap(carrier, year, missedCount);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // add cross year missed connection counts to the final results, then write to output
        for (String c : missedConMap.keySet()) {
            for (int y : missedConMap.get(c).keySet()) {
                if (crossYearMap.containsKey(c) && crossYearMap.get(c).containsKey(y)) {
                    missedConMap.get(c).put(y,
                            missedConMap.get(c).get(y) + crossYearMap.get(c).get(y).countMissedConnections());
                }
                context.write(new Text(c + "," + y), new IntWritable(missedConMap.get(c).get(y)));
            }
        }
    }

    private void updateMissedConMap(String carrier, int year, int c) {
        if (!missedConMap.containsKey(carrier)) {
            missedConMap.put(carrier, new HashMap<Integer, Integer>());
        }

        missedConMap.get(carrier).put(year, c);
    }
    private void updateCrossYearMap(String carrier, int year, ConnectionInfo ci) {
        if (!crossYearMap.containsKey(carrier)) {
            crossYearMap.put(carrier, new HashMap<Integer, CrossYearConnectionInfo>());
        }

        if (!crossYearMap.get(carrier).containsKey(year)) {
            crossYearMap.get(carrier).put(year, new CrossYearConnectionInfo());
        }

        if (!crossYearMap.get(carrier).containsKey(year - 1)) {
            crossYearMap.get(carrier).put(year - 1, new CrossYearConnectionInfo());
        }

        crossYearMap.get(carrier).get(year).updateArrs(ci.lastArrivals());
        crossYearMap.get(carrier).get(year - 1).updateDeps(ci.firstDepartures());
    }
}

