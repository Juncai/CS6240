package analysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.FlightInfo;
import utils.OTPConsts;

import java.io.IOException;

// Authors: Jun Cai and Vikas Boddu
public class AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

//    private static Map<String, Map<Integer, ConnectionInfo>> connectionInfoMap;

//    @Override
//    protected void setup(Context ctx) {
//        connectionInfoMap = new HashMap<String, Map<Integer, ConnectionInfo>>();
//    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // skip the header
        if (line.startsWith(OTPConsts.HEADER_START)) return;

        FlightInfo flight = new FlightInfo(line);

        if (flight.isValid()) {
//            DataPreprocessor.updateConnectionInfoMap(connectionInfoMap, flight);
            context.write(new Text(flight.getCarrier() + " " + flight.getYear()),
                    new Text(flight.getOriginalAirportId() + " "
                            + flight.getDepTimeScheduled().toDate().getTime() + " "
                            + flight.getDepTimeActual().toDate().getTime() + " "
                            + flight.getDestAirportId() + " "
                            + flight.getArrTimeScheduled().toDate().getTime() + " "
                            + flight.getArrTimeActual().toDate().getTime()));
        }
    }

//    @Override
//    protected void cleanup(Context ctx) throws IOException, InterruptedException {
//        Map<Integer, ConnectionInfo> cACMap;
//        ConnectionInfo cci;
//        for (String k : connectionInfoMap.keySet()) {
//            cACMap = connectionInfoMap.get(k);
//            for (Integer ap : cACMap.keySet()) {
//                cci = cACMap.get(ap);
//                for (Date arr : cci.getArrTSs()) {
//                    ctx.write(new Text(k), new Text(ap + " " + arr.getTime()));
//                }
//                for (Date[] c : cci.getCoverRanges()) {
//                    ctx.write(new Text(k), new Text(ap + " " + c[0].getTime() + " " + c[1].getTime()));
//                }
//            }
//
//        }
//    }
}
