package sorting;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Author: Jun Cai
public class Barrier {
    private Map<String, Boolean> stateMap;
    final private Object lock;

    public Barrier(List<String> nodeIPs) {
        stateMap = new HashMap<String, Boolean>();
        lock = new Object();
        for (String ip : nodeIPs) {
            stateMap.put(ip, false);
        }
    }

    private void init() {
        for (String ip : stateMap.keySet()) {
            stateMap.put(ip, false);
        }
    }

    private boolean readyToContinue() {
        boolean ready = true;
        for (String ip : stateMap.keySet()) {
            ready = stateMap.get(ip);
        }
        return ready;
    }

    /***
     * need to make current node into Barrier before call this method
     */
    public void waitForOtherNodes() {
        while (!readyToContinue()) {
            synchronized (lock) {
                try {
                    lock.wait();
                } catch (InterruptedException ex) {
                    // something wrong
                }
            }
        }
        init();
    }

    public void nodeReady(String nodeIP) {
        synchronized (lock) {
            // TODO need to handle dup readies?
            if (stateMap.containsKey(nodeIP)) {
                stateMap.put(nodeIP, true);
                lock.notifyAll();
            }
        }
    }
}
