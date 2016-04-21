package slave;

import hidoop.conf.Configuration;
import hidoop.mapreduce.Partitioner;

/**
 * Created by jon on 4/8/16.
 */
public class PartitionerRunner {
    Configuration conf;
    Partitioner partitioner;
    public PartitionerRunner(String[] configs) {
        // TODO parse configurations

        // TODO prepare context
    }


    public boolean run() {
        // TODO run the partitioner

        return true;
    }
}
