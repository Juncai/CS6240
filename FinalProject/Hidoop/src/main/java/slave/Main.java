package slave;

import hidoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by jon on 4/8/16.
 */
public class Main {
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	
    	
    	int listenPort = conf.slavePort;
    	int nodeInd = Integer.getInteger(args[0]);
    	List<String> ips = conf.slaveIpList;
    	
    	
        // TODO initialize the Communication class
    	SlaveCommunication sc = new SlaveCommunication(listenPort, nodeInd, ips);
    }
}
