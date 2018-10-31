package master.eit.worker;

import master.eit.manager.OnlineManagerWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.CountDownLatch;

public class OnlineWorkerWatcher {
    ZooWorker zw;
    public CountDownLatch onetime = new CountDownLatch(1);

    public OnlineWorkerWatcher(ZooWorker zoo) {
        this.zw = zoo;
        System.out.println("Online worker watcher set");
    }

    public void process(WatchedEvent we) {
        if (we.getType() == Watcher.Event.EventType.NodeDataChanged) {
            System.out.println("Online worker watcher triggered");
            try {
                zw.removeQuit();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        onetime.countDown();
    }

    public void run() {
        synchronized (this) {
            try {
                onetime.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("Online worker watcher ends");
        }
    }
}
