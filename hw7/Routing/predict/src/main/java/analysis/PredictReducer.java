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
        Map<Integer, ConnectionInfo> acMap = new HashMap<Integer, ConnectionInfo>(); // key: airportID, value: connection info
        SimpleFlightInfo flight;
//        String carrier = key.toString();
//        int year = Integer.parseInt(key.toString().split(",")[1]);

        for (Text v : values) {
            flight = new SimpleFlightInfo(v);
            DataPreprocessor.updateConnectionInfo(acMap, flight);
        }

        ConnectionInfo cci;
        for (Integer ap : acMap.keySet()) {
            cci = acMap.get(ap);
            missedCount += cci.countMissedConnections();
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("TruePositive"), new Text(truePos + ""));
        context.write(new Text("TrueNegative"), new Text(trueNeg + ""));
        context.write(new Text("FalsePositive"), new Text(falsePos + ""));
        context.write(new Text("FalseNegative"), new Text(falseNeg + ""));
    }
}

