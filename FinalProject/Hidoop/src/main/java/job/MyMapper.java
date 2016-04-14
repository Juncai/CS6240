package job;

import hidoop.io.Text;
import hidoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jon on 4/8/16.
 */
public class MyMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        System.out.println("Hi from Mapper");
        if (value.toString().length() > 0) {
            context.write(value, new Text("1"));
            System.out.println(value.toString());
        }
    }
}
