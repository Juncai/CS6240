package job.assignment2;

import hidoop.io.DoubleWritable;
import hidoop.io.Text;
import hidoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class OTPReducer extends Reducer<Text, Text, Text, Object> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        if (key.toString().equals(OTPConsts.INVALID)) {
            int sum = 0;
            for (Text value : values) {
                sum += 1;
            }
            context.write(key, new DoubleWritable((double)sum));
        } else {
            Map<String, List<Double>> monthPrices = new HashMap<String, List<Double>>();
            String month;
            Double price;
            List<Double> prices;
            Text carrierMonth = new Text();
            DoubleWritable mean = new DoubleWritable();

			// check if this carrier is still active in 2015
			if (!DataPreprocessor.isActiveCarrier(values)) return;

			int totalFl = 0;
            for (Text value : values) {
				totalFl++;
                month = DataPreprocessor.getMonth(value);
                price = DataPreprocessor.getPrice(value);
                if (!monthPrices.containsKey(month)) {
                    prices = new ArrayList<Double>();
                    prices.add(price);
                    monthPrices.put(month, prices);
                } else {
                    monthPrices.get(month).add(price);
                }
            }

            for (String mk : monthPrices.keySet()) {
                carrierMonth.set(mk + "," + key.toString() + "," + totalFl);
                mean.set(DataPreprocessor.getMean(monthPrices.get(mk)));
                context.write(carrierMonth, mean);
            }
        }
    }
}

