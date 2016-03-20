package analysis;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

// Authors: Jun Cai and Vikas Boddu
public class TrainingReducer extends Reducer<Text, Text, NullWritable, Text> {
	List<String> rfPathList;
    

    @Override
    protected void setup(Context ctx) {
		rfPathList = new ArrayList<String>();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
        String rfPath;
        File f;
        FileWriter fw;
        for (Text v : values) {
            rfPath = "./OTP_prediction_forest_" + UUID.randomUUID().toString();
            rfPathList.add(rfPath);
            f = new File(rfPath);
            f.createNewFile();
            fw = new FileWriter(f, false);
            fw.write(v.toString());
            fw.flush();
            fw.close();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String finalRFPath = "./OTP_prediction_final_" + UUID.randomUUID().toString() + ".rf";
        Process p = Runtime.getRuntime().exec("Rscript ./combineRF.R " + finalRFPath);
        int ret = p.waitFor();

        byte[] b = Files.readAllBytes(Paths.get(finalRFPath));
        String rfString = new String(b, Charset.defaultCharset());
        context.write(NullWritable.get(), new Text(rfString));

        // remove used files
        for (String path : rfPathList) {
            Files.deleteIfExists(Paths.get(path));
        }
        Files.deleteIfExists(Paths.get(finalRFPath));
    }
}
