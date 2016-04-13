package hidoop.mapreduce.lib.input;

import hidoop.mapreduce.Partitioner;

/**
 * Created by alexwang on 4/13/16.
 */
public class defaultPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {
    public int getPartitioner(KEY key, VALUE value, int numPartitions){
        return key.hashCode() % numPartitions;
    }
}
