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
public class PredictMapper extends Mapper<LongWritable, Text, Text, Text> {
    List<String> infoStrList;
    List<String> rInputList;
    List<String> rOutputList;
    long recordCount;

    @Override
    protected void setup(Context context) throws IOException {
        recordCount = 0;
        infoStrList = new ArrayList<String>();
        rInputList = new ArrayList<String>();
        rOutputList = new ArrayList<String>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // skip the header
        if (line.startsWith(OTPConsts.HEADER_START)) return;

        FlightInfo flight = new FlightInfo(line, true);

        if (flight.isValid()) {
            infoStrList.add(flight.toString());
            recordCount++;
        }
        // if infoStrList is large enough, we write it to the rInputFile
        if (infoStrList.size() > 100000) {
            writeRecordsToFile();
        }
    }

    private void writeRecordsToFile() throws IOException {
        String path = "/tmp/OTP_prediction_testing_" + UUID.randomUUID().toString() + ".csv";
        rInputList.add(path);
        File f = new File(path);
        f.createNewFile();
        FileWriter fw = new FileWriter(f, true);
        fw.write(OTPConsts.CSV_HEADER);
        for (String s : infoStrList) {
            fw.write(s);
        }
        fw.flush();
        fw.close();
        infoStrList.clear();
    }

    private void removeUsedFile(List<String> fileList) throws IOException {
        for (String p : fileList) {
            File f = new File(p);
            Files.deleteIfExists(f.toPath());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (recordCount == 0) {
            return;
        }

        if (infoStrList.size() > 0) {
            writeRecordsToFile();
        }
        
        String rfPath = "/tmp/final.rf";
        String outPath;
        String comm;
        Process p;
        int ret;
        for (String inputP : rInputList) {
            outPath = "/tmp/OTP_prediction_result_" + UUID.randomUUID().toString();
            rOutputList.add(outPath);
            comm = "Rscript /tmp/predict.R " + inputP + " " + rfPath + " " + outPath;
            p = Runtime.getRuntime().exec(comm);

            InputStream stdout = p.getInputStream();
            InputStreamReader isr0 = new InputStreamReader(stdout);
            BufferedReader br0 = new BufferedReader(isr0);
            String line0 = null;
            System.out.println("<STD>");
            while ((line0 = br0.readLine()) != null)
                System.out.println(line0);
            System.out.println("</STD>");

            InputStream stderr = p.getErrorStream();
            InputStreamReader isr = new InputStreamReader(stderr);
            BufferedReader br1 = new BufferedReader(isr);
            String line = null;
            System.out.println("<ERROR>");
            while ((line = br1.readLine()) != null)
                System.out.println(line);
            System.out.println("</ERROR>");
            ret = p.waitFor();
            
            System.out.println("R script return with status: " + ret);
            File rOutput = new File(outPath);
            FileReader fr = new FileReader(rOutput);
            BufferedReader br = new BufferedReader(fr);
            String resLine;
            String[] kv;
            while ((resLine = br.readLine()) != null) {
                kv = resLine.split(",");
                context.write(new Text(kv[0]), new Text(kv[1]));
            }
            fr.close();
            br.close();
        }

        // Remove used file from tmp folder
        removeUsedFile(rInputList);
        removeUsedFile(rOutputList);
    }
}
