package job;

import hidoop.io.Text;
import hidoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jon on 4/8/16.
 */
public class MyMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("Hi from Mapper");
    }
}
