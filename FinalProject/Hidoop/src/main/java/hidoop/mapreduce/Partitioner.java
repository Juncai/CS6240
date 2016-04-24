package hidoop.mapreduce;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class Partitioner<KEY, VALUE> {
    public int getPartition(KEY key, VALUE value, int numReduceTasks) {
        return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
