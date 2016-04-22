package hidoop.mapreduce;

import java.util.concurrent.Semaphore;

/**
 * Created by jon on 4/21/16.
 */
public class ReducerInputPool {
    private final Semaphore available;
    private boolean[][] p;

    public ReducerInputPool(int numReducer, int numMapper) {
        p = new boolean[numReducer][numMapper];
        int numInputs = numMapper * numReducer;
        available = new Semaphore(numInputs, true);
        available.acquireUninterruptibly(numInputs);
    }

    public void putInput(int reducerInd, int mapperInd) {
        if (ready(reducerInd, mapperInd)) {
            available.release();
        }
    }

    public void waitTillAllAvailable() throws InterruptedException {
        available.acquireUninterruptibly();
    }

    protected synchronized boolean ready(int rInd, int mInd) {
        if (!p[rInd][mInd]) {
            p[rInd][mInd] = true;
            return true;
        }
        return false;
    }
}
