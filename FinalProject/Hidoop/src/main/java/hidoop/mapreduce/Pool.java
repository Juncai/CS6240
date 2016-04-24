package hidoop.mapreduce;

import hidoop.util.Consts;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;

// Author: Jun Cai
// Reference: github.com/apache/hadoop
public class Pool<T> {
    private final Semaphore available;
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
