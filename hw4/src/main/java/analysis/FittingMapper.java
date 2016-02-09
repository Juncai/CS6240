package analysis;

import org.apache.avro.generic.GenericData;
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
    private Text res = new Text();

    private static List<Double[][]> lom = new ArrayList<Double[][]>();
    private static Map<String, List<Double[][]>> matricesMap;
    private static Map<String, Boolean> isActiveMap;

    @Override
    protected void setup(Context ctx) {
        // initialize the matrices and isIn2015
        Double[][] m1 = new Double[2][2];
        Double[][] m2 = new Double[2][2];

        lom = new ArrayList<Double[][]>();
        lom.add(m1);
        lom.add(m2);

        matricesMap = new HashMap<String, List<Double[][]>>();
        isActiveMap = new HashMap<String, Boolean>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        boolean isActive = DataPreprocessor.processLine(line, carrier, lom);
        if (!carrier.toString().equals(OTPConsts.INVALID)) {
            if (isActive) {
                if (!isActiveMap.containsKey(carrier.toString())) {
                    isActiveMap.put(carrier.toString(), true);
                }
            }

            if (!matricesMap.containsKey(carrier.toString())) {
                matricesMap.put(carrier.toString(), DataPreprocessor.getNewLOM());
            }
            DataPreprocessor.updateMatrices(matricesMap.get(carrier.toString()), lom);
        }
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        // first write active carrier
        for (String c : isActiveMap.keySet()) {
            ctx.write(new Text(c), new Text(OTPConsts.ACTIVE));
        }
        for (String c : matricesMap.keySet()) {
            ctx.write(new Text(c), new Text(DataPreprocessor.serializeMatrices(matricesMap.get(c))));
        }
    }
}
