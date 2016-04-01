package sorting;

// Author: Jun Cai
public class SingleServerBarrier {
    private boolean[] sampleStates;
    private boolean[] dataStates;
    private int nNodes;
    final private Object lock;

    public SingleServerBarrier(int nNodes) {
        sampleStates = new boolean[nNodes];
        dataStates = new boolean[nNodes];
        lock = new Object();
        this.nNodes = nNodes;
        init();
    }

    private void init() {
        for (int i = 0; i < nNodes; i++) {
            sampleStates[i] = false;
            dataStates[i] = false;
        }
    }

    private boolean readyToContinue(Consts.Stage stage) {
        boolean[] states = (stage == Consts.Stage.SELECT) ? sampleStates : dataStates;
        boolean ready = true;
        for (boolean state : states) {
            ready = ready && state;
        }
        return ready;
    }

    /***
     * need to make current node into Barrier before call this method
     */
    public void waitForOtherNodes(Consts.Stage stage) {
        System.out.println("Number of nodes in barrier: " + nNodes);

        while (!readyToContinue(stage)) {
            synchronized (lock) {
                try {
                    lock.wait();
                } catch (InterruptedException ex) {
                    // something wrong
                }
            }
        }
        // since we separate the stages, no init() needed
//        init();
    }

    public void nodeReady(int nodeInd, Consts.Stage stage) {
        boolean[] states = (stage == Consts.Stage.SELECT) ? sampleStates : dataStates;
        synchronized (lock) {
            // TODO need to handle dup readies?
            if (nodeInd >= 0 && nodeInd < states.length) {
                states[nodeInd] = true;
                lock.notifyAll();
            }
        }
    }
}
