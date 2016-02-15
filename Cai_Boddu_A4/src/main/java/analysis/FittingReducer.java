package analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.DataPreprocessor;
import utils.OTPConsts;

import java.io.IOException;
import java.util.List;


public class FittingReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        // check if this carrier is still active in 2015
        if (!DataPreprocessor.isActiveCarrier(values)) return;
        List<Double[][]> rlomD = DataPreprocessor.getNewLOM();
        List<Double[][]> rlomT = DataPreprocessor.getNewLOM();
        List<Double[][]> lom;
        for (Text val : values) {
            String valString = val.toString();
            if (valString.equals(OTPConsts.ACTIVE)) continue;
            lom = DataPreprocessor.deserializeMatrices(valString.split(" ")[1]);
            if (valString.startsWith("D ")) {
                DataPreprocessor.updateMatrices(rlomD, lom);
            }
            if (valString.startsWith("T ")) {
                DataPreprocessor.updateMatrices(rlomT, lom);
            }
        }
        double[][] thetaD = DataPreprocessor.fit(rlomD);
        double[][] thetaT = DataPreprocessor.fit(rlomT);
        context.write(key, new Text("D," + DataPreprocessor.serializeMatrix(thetaD)));
        context.write(key, new Text("T," + DataPreprocessor.serializeMatrix(thetaT)));
//        context.write(key, new Text(DataPreprocessor.serializeMatrices(rlomD)));
//        context.write(key, new Text(DataPreprocessor.serializeMatrices(rlomT)));
    }
}

