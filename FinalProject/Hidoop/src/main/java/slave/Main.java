package slave;

import hidoop.conf.Configuration;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by jon on 4/8/16.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        // for testing
//		Configuration conf = new Configuration(true);
//		if (args[0].equals("s")) {
//			// server side
//			ServerSocket server = new ServerSocket(10001);
//			Socket s = server.accept();
//			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
//			Configuration c = (Configuration) ois.readObject();
//			System.out.println(c.mapperClass.getCanonicalName());
//			ois.close();
//
//		} else {
//			// client side
//			Socket s = new Socket("127.0.0.1", 10001);
//			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
//			oos.writeObject(conf);
//			oos.flush();
//			oos.close();
//			s.close();
//		}

		int nodeInd = Integer.getInteger(args[0]);
		int listeningPort = Integer.parseInt(args[1]);
		String masterIp = args[2];
		int masterPort = Integer.parseInt(args[3]);
        // TODO initialize the Communication class
    	SlaveCommunication sc = new SlaveCommunication(nodeInd, listeningPort, masterIp, masterPort);
		sc.start();
		// TODO wait for job completed
    }
}
