package analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

// Authors: Jun Cai and Vikas Boddu
public class TrainingReducer extends Reducer<Text, Text, Text, Text> {
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
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        // TODO write forests to file
//        List<String> rfPathList = new ArrayList<String>();
//        String rfPath;
//        File f;
//        FileWriter fw;
//        for (String s : rfList) {
//            rfPath = "/tmp/OTP_prediction_forest_" + UUID.randomUUID().toString();
//            rfPathList.add(rfPath);
//            f = new File(rfPath);
//            f.createNewFile();
//            fw = new FileWriter(f, false);
//            fw.write(s);
//            fw.flush();
//            fw.close();
//        }
//        // TODO call R script to combine the forests in to one final forest
//        Runtime.getRuntime().exec("Rscript /tmp/combineRF.R");
//
//        // read final forest string and write it to the context
//        String finalRFPath = "/tmp/OTP_prediction_final.rf";
//        byte[] b = Files.readAllBytes(Paths.get(finalRFPath));
//        String rfString = new String(b, Charset.defaultCharset());
//        context.write(new Text("FinalRF"), new Text(rfString));
//
//        // remove used files
//        for (String p : rfPathList) {
//            Files.deleteIfExists(Paths.get(p));
//        }
//        Files.deleteIfExists(Paths.get(finalRFPath));
    }
}

