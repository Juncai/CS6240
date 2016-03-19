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
        // TODO write forests to file
        List<String> rfPathList = new ArrayList<String>();
        String rfPath;
        File f;
        FileWriter fw;
        for (String s : rfList) {
            rfPath = "/tmp/OTP_prediction_forest_" + UUID.randomUUID().toString();
            rfPathList.add(rfPath);
            f = new File(rfPath);
            f.createNewFile();
            fw = new FileWriter(f, false);
            fw.write(s);
            fw.flush();
            fw.close();
        }
        // TODO call R script to combine the forests in to one final forest
        String finalRFPath = "/tmp/OTP_prediction_final_" + UUID.randomUUID().toString() + ".rf";
        Process p = Runtime.getRuntime().exec("Rscript /tmp/combineRF.R " + finalRFPath);

       // InputStream stdout = p.getInputStream();
       // InputStreamReader isr0 = new InputStreamReader(stdout);
       // BufferedReader br0 = new BufferedReader(isr0);
       // String line0 = null;
       // System.out.println("<STD>");
       // while ((line0 = br0.readLine()) != null)
       //     System.out.println(line0);
       // System.out.println("</STD>");

       // InputStream stderr = p.getErrorStream();
       // InputStreamReader isr = new InputStreamReader(stderr);
       // BufferedReader br = new BufferedReader(isr);
       // String line = null;
       // System.out.println("<ERROR>");
       // while ((line = br.readLine()) != null)
       //     System.out.println(line);
       // System.out.println("</ERROR>");

        int ret = p.waitFor();
       // System.out.println("R script return with status: " + ret);

        // read final forest string and write it to the context
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

