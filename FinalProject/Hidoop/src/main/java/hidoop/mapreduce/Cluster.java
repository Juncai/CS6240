package hidoop.mapreduce;

import hidoop.conf.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class Cluster {
    private Configuration conf;
    private Client client;

    public Cluster(Configuration conf) throws IOException {
        this.conf = conf;
        initialize(conf);
    }

    private void initialize(Configuration conf) throws IOException {
        if (conf.isLocalMode) {
            client = new LocalClient(conf);
        } else {
            client = new EC2Client(conf);
        }
    }

    Client getClient() {
        return client;
    }

    Configuration getConf() {
        return conf;
    }

}
