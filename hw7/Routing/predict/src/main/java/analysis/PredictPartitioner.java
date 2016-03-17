package analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

// Authors: Jun Cai and Vikas Boddu
public class PredictPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        String carrier = key.toString().split(",")[0];
        return (carrier.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
