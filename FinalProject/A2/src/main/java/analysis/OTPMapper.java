package analysis;
 
import hidoop.io.IntWritable;
 import hidoop.io.LongWritable;
 import hidoop.io.Text;
 import hidoop.mapreduce.Mapper;
 
 import java.io.IOException;
 
 /**
  * Created by phoenix on 1/28/16.
  */
 
 public class OTPMapper extends Mapper<Object, Text, Text, Text> {
     private Text carrier = new Text();
     private Text dateAndPrice = new Text();
 
     @Override
     public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         DataPreprocessor.processLine(line, carrier, dateAndPrice);
         context.write(carrier, dateAndPrice);
     }
 }