package job.assignment5.analysis;

import hidoop.io.Text;
import hidoop.mapreduce.Partitioner;

// Authors: Jun Cai and Vikas Boddu
public class AnalysisPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        String carrier = key.toString().split(",")[0];
        return (carrier.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
