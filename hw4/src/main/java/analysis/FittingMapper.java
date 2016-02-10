package analysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.DataPreprocessor;
import utils.OTPConsts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Jun Cai on 2/08/16.
 */
public class FittingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text carrier = new Text();
    // for mapper output, we need if this carrier is active in 2015, x_t*x and x_t*y

    private static Map<String, List<Double[][]>> distanceMap;
    private static Map<String, List<Double[][]>> timeMap;
    private static Map<String, Boolean> isActiveMap;

    @Override
    protected void setup(Context ctx) {
        // initialize the matrices and isIn2015
        distanceMap = new HashMap<String, List<Double[][]>>();
        timeMap = new HashMap<String, List<Double[][]>>();
        isActiveMap = new HashMap<String, Boolean>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        List<Double[][]> dLOM = new ArrayList<Double[][]>();
        List<Double[][]> tLOM = new ArrayList<Double[][]>();
        boolean isActive = DataPreprocessor.processLine(line, carrier, dLOM, tLOM);
        if (!carrier.toString().equals(OTPConsts.INVALID)) {
            if (isActive) {
                if (!isActiveMap.containsKey(carrier.toString())) {
                    isActiveMap.put(carrier.toString(), true);
                }
            }

            if (!distanceMap.containsKey(carrier.toString())) {
                distanceMap.put(carrier.toString(), DataPreprocessor.getNewLOM());
                timeMap.put(carrier.toString(), DataPreprocessor.getNewLOM());
            }
            DataPreprocessor.updateMatrices(distanceMap.get(carrier.toString()), dLOM);
            DataPreprocessor.updateMatrices(timeMap.get(carrier.toString()), tLOM);
        }
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        // first write active carrier
        for (String c : isActiveMap.keySet()) {
            ctx.write(new Text(c), new Text(OTPConsts.ACTIVE));
        }
        for (String c : distanceMap.keySet()) {
//            System.out.format("Carrier: %s, xtx: %s\n", c, DataPreprocessor.serializeMatrices(distanceMap.get(c)));
            ctx.write(new Text(c), new Text("D " + DataPreprocessor.serializeMatrices(distanceMap.get(c))));
            ctx.write(new Text(c), new Text("T " + DataPreprocessor.serializeMatrices(timeMap.get(c))));
        }
    }
}
