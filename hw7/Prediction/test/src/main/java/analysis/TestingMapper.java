package analysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.FlightInfo;
import utils.OTPConsts;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

// Authors: Jun Cai and Vikas Boddu
public class TestingMapper extends Mapper<LongWritable, Text, Text, Text> {
    
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

        // two possible input format: 1. test input, 2. validate input
        String[] splits = line.split(",");
        if (splits.length == 2) {
            context.write(new Text(splits[0]), new Text(splits[1]));
        } else {
            FlightInfo flight = new FlightInfo(line, true);

            if (flight.isValid()) {
                infoStrList.add(flight.toString());
                recordCount++;
            }
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
        // this Mapper may deal with pure validate data
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
            comm = "Rscript /tmp/validate.R " + inputP + " " + rfPath + " " + outPath;
            p = Runtime.getRuntime().exec(comm);
            ret = p.waitFor();
            
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
