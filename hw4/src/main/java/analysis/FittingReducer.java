package analysis;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.DataPreprocessor;
import utils.OTPConsts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FittingReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        // check if this carrier is still active in 2015
        if (!DataPreprocessor.isActiveCarrier(values)) return;
        double[][] xtx = new double[2][2];
        double[][] xty = new double[2][2];
        List<Double[][]> lom;
        for (Text val : values) {
            lom = DataPreprocessor.deserializeMatrices(val.toString());
            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < 2; j++) {
                    xtx[i][j] += lom.get(0)[i][j];
                    xty[i][j] += lom.get(1)[i][j];
                }
            }
        }
        double[][] theta = DataPreprocessor.fit(xtx, xty);
        context.write(key, new Text(DataPreprocessor.serializeMatrix(theta)));
    }
}

