package hidoop.mapreduce;

import java.util.concurrent.Semaphore;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class ReducerInputPool {
    private final Semaphore available;
    private boolean[][] p;
    private int numInputs;

    public ReducerInputPool(int numReducer, int numMapper) {
        p = new boolean[numReducer][numMapper];
        numInputs = numMapper * numReducer;
        available = new Semaphore(numInputs, true);
        waitTillAllAvailable();
    }

    public void putInput(int reducerInd, int mapperInd) {
        if (ready(reducerInd, mapperInd)) {
            available.release();
            System.out.println("available: " + available.availablePermits());
        }
    }

    public void waitTillAllAvailable() {
        available.acquireUninterruptibly(numInputs);
    }

    protected synchronized boolean ready(int rInd, int mInd) {
        if (!p[rInd][mInd]) {
            p[rInd][mInd] = true;
            return true;
        }
        return false;
    }
}
