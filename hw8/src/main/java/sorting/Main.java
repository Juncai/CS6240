package sorting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Author:
public class Main {
    public static void main(String[] args) throws Exception {
//        int localPort = 10001;
        int localPort = Integer.parseInt(args[0]);
        List<String> ipList = new ArrayList<String>();
        ipList.add(args[1]);
//        ipList.add("127.0.0.1:" + args[1]);
//        ipList.add("127.0.0.1:10003");
//        ipList.add("127.0.0.1:10004");
//        ipList.add("127.0.0.1:10005");
//        ipList.add("127.0.0.1:10006");
//        ipList.add("127.0.0.1:10007");
//        ipList.add("127.0.0.1:10008");

        // create communication object and initialize it
        System.out.println("Start creating NodeCommunication...");
        // TODO handle communication with master node
        String masterAddr = "127.0.0.1:10000";
        NodeCommunication comm = new NodeCommunication(localPort, masterAddr, ipList);

        // TODO sample local data

        // send data to other nodes
        List<String> dataToSomeNode = new ArrayList<String>();
        dataToSomeNode.add("The sample data, handsome!");
        System.out.println("Start sending sample data...");
        List<String> dataReceived;
        for (String adr : ipList) {
            String ip = adr.split(":")[0];
            comm.sendDataToNode(ip, dataToSomeNode);
        }
        // enter barrier, wait for other nodes getting ready for SELECT stage
        System.out.println("Entering barrier...");
        comm.barrier(Consts.Stage.SELECT);
        // load data from buffer
        System.out.println("Start reading sample data...");
        dataReceived = comm.readBufferedData();

        // TODO combine sample data, choose pivots, prepare data for other nodes

        // send data to other nodes
        System.out.println("Start sending select data...");
        for (String adr : ipList) {
            String ip = adr.split(":")[0];
            comm.sendDataToNode(ip, dataToSomeNode);
        }
        // enter barrier, wait for other nodes getting ready for SORT stage
        System.out.println("Entering barrier...");
        comm.barrier(Consts.Stage.SORT);
        // load data from buffer
        System.out.println("Start reading select data...");
        dataReceived = comm.readBufferedData();

        // TODO sort the local data, then send the result to master node

        // close sockets
        System.out.println("Closing connections...");
        comm.endCommunication();
    }
}
