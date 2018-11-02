package master.eit.manager;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.CountDownLatch;

public class OnlineManagerWatcher implements Runnable, Watcher{

    ZooManager zm;
    CountDownLatch onetime = new CountDownLatch(1);


    public OnlineManagerWatcher(ZooManager zoo) {
        this.zm = zoo;
        System.out.println("Online Manager Watcher set");

    }

    public void process(WatchedEvent we) {
        if (we.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            System.out.println("Online Manager Watcher triggered !!");
            try {
                zm.goOnline();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        onetime.countDown();
    }

    public void run() {
        synchronized(this) {
            try {
                onetime.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("Online Manager Watcher thread ends");
        }
    }
}
