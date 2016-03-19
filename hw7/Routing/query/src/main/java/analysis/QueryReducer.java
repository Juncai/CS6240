package analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.ConnectionInfo;
import utils.DataPreprocessor;
import utils.OTPConsts;
import utils.SimpleFlightInfo;

import java.io.*;
import java.util.*;

// Authors: Jun Cai and Vikas Boddu
public class QueryReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void setup(Context ctx) {
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean hasRequest = false;
        List<String[]> candidates = new ArrayList<String[]>();
        String valueStr;
        for (Text v : values) {
            valueStr = v.toString();
            if (valueStr.equals("Q")) {
                hasRequest = true;
            } else {
                candidates.add(valueStr.split(","));
            }
        }

        if (hasRequest && candidates.size() > 0) {
            int minExpDuration = Integer.parseInt(candidates.get(0)[2]);
            int duration = Integer.parseInt(candidates.get(0)[3]);
            String flNum1 = candidates.get(0)[0];
            String flNum2 = candidates.get(0)[1];

            for (int i = 1; i < candidates.size(); i++) {
                int cDuration = Integer.parseInt(candidates.get(i)[2]);
                if (cDuration < minExpDuration) {
                    minExpDuration = cDuration;
                    flNum1 = candidates.get(i)[0];
                    flNum2 = candidates.get(i)[1];
                    duration = Integer.parseInt(candidates.get(i)[3]);
                }
            }
            context.write(key, new Text(flNum1 + OTPConsts.COMMA + flNum2 + OTPConsts.COMMA + duration));
        }

    }
}

