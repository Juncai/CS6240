package analysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.FlightInfo;
import utils.OTPConsts;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

// Authors: Jun Cai and Vikas Boddu
public class QueryMapper extends Mapper<LongWritable, Text, Text, Text> {
    List<String> infoStrList;

    @Override
    protected void setup(Context context) {
        infoStrList = new ArrayList<String>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // skip the header
        if (line.startsWith(OTPConsts.HEADER_START)) return;

        // two possible input format: 1. test input, 2. validate input
        String[] splits = line.split(",");
        if (splits.length == 2) {
            context.write(new Text(splits[0]), new Text(splits[1]));
        } else {
            FlightInfo flight = new FlightInfo(line, true);

            if (flight.isValid()) {
                infoStrList.add(flight.toString());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String fName = "/tmp/OTP_prediction_testing_" + UUID.randomUUID().toString() + ".csv";
        File f = new File(fName);
        f.createNewFile();
        FileWriter fw = new FileWriter(f, true);
        fw.write(OTPConsts.CSV_HEADER);
        for (String s : infoStrList) {
            fw.write(s);
        }
        fw.flush();
        fw.close();

        // TODO call R script to make prediction on the test data
        String outPath = "/tmp/OTP_prediction_result_" + UUID.randomUUID().toString();
        String rfPath = "/tmp/final.rf";
        String comm = "Rscript /tmp/predict.R " + fName + " " + rfPath + " " + outPath;
//        System.out.println(comm);
        Process p = Runtime.getRuntime().exec(comm);

//        InputStream stdout = p.getInputStream();
//        InputStreamReader isr0 = new InputStreamReader(stdout);
//        BufferedReader br0 = new BufferedReader(isr0);
//        String line0 = null;
//        System.out.println("<STD>");
//        while ((line0 = br0.readLine()) != null)
//            System.out.println(line0);
//        System.out.println("</STD>");
//
//        InputStream stderr = p.getErrorStream();
//        InputStreamReader isr = new InputStreamReader(stderr);
//        BufferedReader br1 = new BufferedReader(isr);
//        String line = null;
//        System.out.println("<ERROR>");
//        while ((line = br1.readLine()) != null)
//            System.out.println(line);
//        System.out.println("</ERROR>");

        int ret = p.waitFor();
//        System.out.println("R script return with status: " + ret);

        // TODO read lines from the R output, write each line to the context
        // R output format: [FL_NUM]_[FL_DATE]_[CRS_DEP_TIME],logical
        File rOutput = new File(outPath);
        FileReader fr = new FileReader(rOutput);
        BufferedReader br = new BufferedReader(fr);
        String resLine;
        String[] kv;
        while ((resLine = br.readLine()) != null) {
//            kv = DataPreprocessor.parseROutput(resLine);
            kv = resLine.split(",");
            context.write(new Text(kv[0]), new Text(kv[1]));
        }
        fr.close();
        br.close();

        // Remove used file from tmp folder
        Files.deleteIfExists(f.toPath());
        Files.deleteIfExists(Paths.get(outPath));
    }
}
