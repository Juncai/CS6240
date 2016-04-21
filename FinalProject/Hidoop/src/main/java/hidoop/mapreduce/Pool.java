package hidoop.mapreduce;

import hidoop.util.Consts;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * Created by jon on 4/21/16.
 */
public class Pool<T> {
    private Semaphore available;
    private Queue<T> q;

    public Pool(int numOfResource) {
        q = new ArrayBlockingQueue<T>(numOfResource);
        available = new Semaphore(numOfResource, true);
        available.acquireUninterruptibly(numOfResource);
    }

    public void putResource(T t) {
        if (enqueue(t)) {
            available.release();
        }
    }

    public T getResource() throws InterruptedException {
        available.acquire();
        return dequeue();
    }

    public void waitTillAllAvailable() throws InterruptedException {
        available.acquireUninterruptibly();
    }

    protected synchronized boolean enqueue(T t) {
        if (Node.class.isInstance(t)) {
            Node n = (Node)t;
            n.status = Consts.NodeStatus.IDLE;
            n = null;
        }
        q.add(t);
        return true;
    }

    protected  synchronized T dequeue() {
        if (q.isEmpty()) {
            return null;
        } else {
            return q.remove();
        }
    }
}
