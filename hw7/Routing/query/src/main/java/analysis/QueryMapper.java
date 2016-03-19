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

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // skip the header
        if (line.startsWith(OTPConsts.HEADER_START)) return;

        // two possible input format: 1. prediction input, 2. query input
        String k;
        String v;
        String[] splits = line.split(",");
        if (line.startsWith("P")) {
            // handle prediction input
            k = extractKey(splits, 1, 5);
            v = splits[6] + OTPConsts.COMMA
                    + splits[7] + OTPConsts.COMMA
                    + splits[8] + OTPConsts.COMMA
                    + splits[9];
            context.write(new Text(k), new Text(v));
        } else {
            k = extractKey(splits, 0, -1);
            v = "Q";
            context.write(new Text(k), new Text(v));
        }
    }

    private String extractKey(String[] s, int startInd, int endInd) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        int end = (endInd == -1) ? s.length - 1 : endInd;
        for (String ss : s) {
            if (i >= startInd && i <= end) {
                sb.append(ss);
                if (i < end) {
                    sb.append(OTPConsts.COMMA);
                }
            }
        }
        return sb.toString();
    }
}
