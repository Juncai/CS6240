package hidoop.mapreduce;

/**
 * Created by jon on 4/6/16.
 */
public abstract class Partitioner<KEY, VALUE> {
    public abstract int getPartitioner(KEY key, VALUE value, int numPartitions);
}
