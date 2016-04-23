package hidoop.mapreduce;

import java.util.concurrent.Semaphore;

/**
 * Created by jon on 4/21/16.
 */
public class ReducerInputPool {
    private final Semaphore available;
    private boolean[][] p;
    private int numInputs;

    public ReducerInputPool(int numReducer, int numMapper) {
        p = new boolean[numReducer][numMapper];
        numInputs = numMapper * numReducer;
        available = new Semaphore(numInputs, true);
//        available.acquireUninterruptibly(numInputs);
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
