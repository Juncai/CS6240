package analysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.DataPreprocessor;
import utils.OTPConsts;

import java.io.IOException;

/**
 * Created by phoenix on 1/28/16.
 */
public class OTPMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text carrier = new Text();
    private Text dateAndPrice = new Text();
    private static int badRecords;

    @Override
    protected void setup(Context ctx) {
        badRecords = 0;
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        DataPreprocessor.processLine(line, carrier, dateAndPrice);
        if (carrier.toString().equals(OTPConsts.INVALID)) {
            badRecords++;
        } else {
            context.write(carrier, dateAndPrice);
        }
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        ctx.write(new Text(OTPConsts.INVALID), new Text(badRecords + ""));
    }
}
