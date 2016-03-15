package analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Authors: Jun Cai and Vikas Boddu
public class TrainingReducer extends Reducer<Text, Text, Text, IntWritable> {
    List<String> rfList;

    @Override
    protected void setup(Context ctx) {
        rfList = new ArrayList<String>();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) {
        for (Text v : values) {
            rfList.add(v.toString());
        }
    }

    @Override
    protected void cleanup(Context context) {
        // TODO call R script to combine the forests in to one final forest


        // TODO read final forest string and write it to the context

    }
}

