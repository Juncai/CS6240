package hidoop.mapreduce;

import hidoop.conf.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by jon on 4/7/16.
 */
public class Cluster {
    private Configuration conf;

    public Cluster(Configuration conf) throws IOException {
        this.conf = conf;
//        initialize(jobTrackAddr, conf);
    }


//    private void initialize(InetSocketAddress jobTrackAddr, Configuration conf)
//            throws IOException {
//
//        synchronized (frameworkLoader) {
//            for (ClientProtocolProvider provider : frameworkLoader) {
//                LOG.debug("Trying ClientProtocolProvider : "
//                        + provider.getClass().getName());
//                ClientProtocol clientProtocol = null;
//                try {
//                    if (jobTrackAddr == null) {
//                        clientProtocol = provider.create(conf);
//                    } else {
//                        clientProtocol = provider.create(jobTrackAddr, conf);
//                    }
//
//                    if (clientProtocol != null) {
//                        clientProtocolProvider = provider;
//                        client = clientProtocol;
//                        LOG.debug("Picked " + provider.getClass().getName()
//                                + " as the ClientProtocolProvider");
//                        break;
//                    } else {
//                        LOG.debug("Cannot pick " + provider.getClass().getName()
//                                + " as the ClientProtocolProvider - returned null protocol");
//                    }
//                } catch (Exception e) {
//                    LOG.info("Failed to use " + provider.getClass().getName()
//                            + " due to error: " + e.getMessage());
//                }
//            }
//        }
//
//        if (null == clientProtocolProvider || null == client) {
//            throw new IOException(
//                    "Cannot initialize Cluster. Please check your configuration for "
//                            + MRConfig.FRAMEWORK_NAME
//                            + " and the correspond server addresses.");
//        }
//    }
//
//    ClientProtocol getClient() {
//        return client;
//    }

    Configuration getConf() {
        return conf;
    }

}
