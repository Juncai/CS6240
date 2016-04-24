package analysis;

import hidoop.io.Text;
import hidoop.mapreduce.Mapper;

import utils.FlightInfo;
import utils.OTPConsts;

import java.io.IOException;

// Authors: Jun Cai and Vikas Boddu
public class AnalysisMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // skip the header
        if (line.startsWith(OTPConsts.HEADER_START)) return;

        FlightInfo flight = new FlightInfo(line);

        if (flight.isValid()) {
            context.write(new Text(flight.getCarrier() + "," + flight.getYear()),
                    new Text(flight.getOriginalAirportId() + " "
                            + flight.getDepTimeScheduled().toDate().getTime() + " "
                            + flight.getDepTimeActual().toDate().getTime() + " "
                            + flight.getDestAirportId() + " "
                            + flight.getArrTimeScheduled().toDate().getTime() + " "
                            + flight.getArrTimeActual().toDate().getTime()));
        }
    }
}
