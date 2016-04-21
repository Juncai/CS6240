package job.my;

import hidoop.io.Text;
import hidoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jon on 4/8/16.
 */
public class MyReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
//        System.out.println("Hi from Reducer!");
        int count = 0;
        for (Text v : values) {
            count++;
        }
        context.write(key, new Text(count + ""));
    }
}
