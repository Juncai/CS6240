package analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.ConnectionInfo;
import utils.DataPreprocessor;
import utils.FlightInfo;
import utils.OTPConsts;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Authors: Jun Cai and Vikas Boddu
public class AnalysisReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        Map<Integer, ConnectionInfo> acMap = new HashMap<Integer, ConnectionInfo>(); // key: airportID, value: connection info
        FlightInfo flight;
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
        }

        context.write(key, new IntWritable(missedCount));
    }
}

