package analysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by phoenix on 1/28/16.
 */

public class OTPMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text carrier = new Text();
    private Text dateAndPrice = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        DataPreprocessor.processLine(line, carrier, dateAndPrice);
        context.write(carrier, dateAndPrice);
    }
}
