package master.eit.manager;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;



public class RegisterManagerWatcher implements Watcher, Runnable {

    ZooManager zm;
    CountDownLatch onetime = new CountDownLatch(1);


    public RegisterManagerWatcher(ZooManager zoo) {
        this.zm = zoo;
        System.out.println("Register Manager Watcher set");

    }

    //it get triggered twice for every registation process, once when the new node-request
    // is created , and once when it is deleted. This last time, the call to register() doesn't have
    // any effects, because  the childreen list is empty now.
    public void process(WatchedEvent we) {
        if (we.getType() == EventType.NodeChildrenChanged) {
            System.out.println("Register Manager Watcher triggered !!");
            try {
                zm.register();
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
            System.out.println("Register Manager Watcher thread ends");
        }
    }
}