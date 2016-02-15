package analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.ConnectionInfo;
import utils.DataPreprocessor;
import utils.OTPConsts;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class AnalysisReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        Map<Integer, ConnectionInfo> acMap = new HashMap<Integer, ConnectionInfo>();

        for (Text v : values) {
            DataPreprocessor.updateConnectionInfo(acMap, v.toString());
        }

        int missedCount = 0;
        ConnectionInfo cci;
        for (Integer ap : acMap.keySet()) {
            cci = acMap.get(ap);
            cci.processCoverRangesInReducer();
            missedCount += cci.getArrTSs().size();
        }

        context.write(key, new IntWritable(missedCount));
    }
}

