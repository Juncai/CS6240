package hidoop.mapreduce;

/**
 * Created by jon on 4/6/16.
 */
public class Partitioner<KEY, VALUE> {
    public int getPartition(KEY key, VALUE value, int numReduceTasks) {
        return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
