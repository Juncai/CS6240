package analysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.FlightInfo;
import utils.OTPConsts;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

// Authors: Jun Cai and Vikas Boddu
public class TrainingMapper extends Mapper<LongWritable, Text, Text, Text> {
    List<FlightInfo> infoList;

    @Override
    protected void setup(Context context) {
        infoList = new ArrayList<FlightInfo>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // skip the header
        if (line.startsWith(OTPConsts.HEADER_START)) return;

        FlightInfo flight = new FlightInfo(line);

        if (flight.isValid()) {
            infoList.add(flight);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String fName = "/tmp/OTP_prediction_training_" + UUID.randomUUID().toString() + ".csv";
        File f = new File(fName);
        f.createNewFile();
        FileWriter fw = new FileWriter(f, true);
        fw.write(OTPConsts.CSV_HEADER);
        for (FlightInfo i : infoList) {
            fw.write(i.toString());
        }
        fw.flush();
        fw.close();

        // TODO call R script to get Random Forest
        String path = "/tmp/OTP_prediction_" + UUID.randomUUID().toString() + ".rf";
        Runtime.getRuntime().exec("Rscript /tmp/rf.R " + fName + " " + path);

        // read Random Forest as string and write it to the context
        byte[] b = Files.readAllBytes(Paths.get(path));
        String rfString = new String(b, Charset.defaultCharset());
        context.write(new Text("RandomForest"), new Text(rfString));

        // Remove used file from tmp folder
        Files.deleteIfExists(f.toPath());
        Files.deleteIfExists(Paths.get(path));
    }
}
