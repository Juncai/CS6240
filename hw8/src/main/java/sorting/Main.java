package sorting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jon on 3/23/16.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        int localPort = 10001;
        List<String> ipList = new ArrayList<String>();
        ipList.add("localhost:10002");
        ipList.add("localhost:10003");
//        ipList.add("localhost:10004");
//        ipList.add("localhost:10005");
//        ipList.add("localhost:10006");
//        ipList.add("localhost:10007");
//        ipList.add("localhost:10008");

        // create communication object and initialize it
        // TODO handle communication with master node
        String masterAddr = "localhost:10000";
        NodeCommunication comm = new NodeCommunication(localPort, masterAddr, ipList);

        // TODO sample local data

        // send data to other nodes
        List<String> dataToSomeNode = new ArrayList<String>();
        List<String> dataReceived;
        for (String ip : ipList) {
            comm.sendDataToNode(ip, dataToSomeNode);
        }
        // enter barrier, wait for other nodes getting ready for SELECT stage
        comm.barrier(Consts.Stage.SELECT);
        // load data from buffer
        dataReceived = comm.readBufferedData();

        // TODO combine sample data, choose pivots, prepare data for other nodes

        // send data to other nodes
        for (String ip : ipList) {
            comm.sendDataToNode(ip, dataToSomeNode);
        }
        // enter barrier, wait for other nodes getting ready for SORT stage
        comm.barrier(Consts.Stage.SORT);
        // load data from buffer
        dataReceived = comm.readBufferedData();

        // TODO sort the local data, then send the result to master node

        // close sockets
        comm.endCommunication();
    }
}
