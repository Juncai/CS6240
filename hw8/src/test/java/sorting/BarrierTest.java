package sorting;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by jon on 3/23/16.
 */
public class BarrierTest {

    @org.junit.Test
    public void testWaitForOtherNodes() throws Exception {
        List<String> ipList = new ArrayList<String>();
        ipList.add("ip0:port0");
        ipList.add("ip1:port1");
        ipList.add("ip2:port2");
        ipList.add("ip3:port3");
        ipList.add("ip4:port4");
        ipList.add("ip5:port5");
        Barrier b = new Barrier(ipList);
        for (String ad : ipList) {
            TestThread tt = new TestThread(ad, b);
            tt.start();
        }
        System.out.println("Wait for other nodes...");
        b.waitForOtherNodes();
        System.out.println("Done!");

    }

    private class TestThread extends Thread {
        private String addr;
        private Barrier b;
        public TestThread(String addr, Barrier b) {
            this.addr = addr;
            this.b = b;
        }

        public void run() {
            try {
                sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            b.nodeReady(addr);
        }
    }
}