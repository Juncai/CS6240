package slave;


// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class Main {
    public static void main(String[] args) throws Exception {

		int nodeInd = Integer.parseInt(args[0]);
		int listeningPort = Integer.parseInt(args[1]);
		String masterIp = args[2];
		int masterPort = Integer.parseInt(args[3]);
        // TODO initialize the Communication class
    	SlaveCommunication sc = new SlaveCommunication(nodeInd, listeningPort, masterIp, masterPort);
		sc.start();
		// TODO wait for job completed
		sc.waitTillJobCompleted();
    }
}
