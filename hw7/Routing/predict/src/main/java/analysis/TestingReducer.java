package analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;

// Authors: Jun Cai and Vikas Boddu
public class TestingReducer extends Reducer<Text, Text, Text, Text> {
    long truePos;
    long trueNeg;
    long falsePos;
    long falseNeg;

    @Override
    protected void setup(Context ctx) {
        truePos = 0;
        trueNeg = 0;
        falsePos = 0;
        falseNeg = 0;
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        boolean delayActual = false;
        boolean delayPredict = false;
        String vStr;
        for (Text v : values) {
            count++;
            vStr = v.toString();
            if (vStr.equals("TRUE")) {
                delayActual = true;
            } else if (vStr.equals("FALSE")) {
                delayActual = false;
            } else if (vStr.equals("1")) {
                delayPredict = true;
                context.write(key, new Text("TRUE"));
            } else if (vStr.equals("0")) {
                delayPredict = false;
                context.write(key, new Text("FALSE"));
            }
        }
        if (count == 2) {
            if (delayActual) {
                if (delayPredict) {
                    truePos++;
                } else {
                    falseNeg++;
                }
            } else {
                if (delayPredict) {
                    falsePos++;
                } else {
                    trueNeg++;
                }
            }
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

