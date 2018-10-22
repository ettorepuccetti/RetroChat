package master.eit;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static master.eit.ZooManager.zoo;


public class ZooWorker {

    public static class myWatcher implements Watcher{
        public void process(WatchedEvent watchedEvent) {
        System.out.printf("\nEvent Received: %s", watchedEvent.toString());
        if (watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            if (watchedEvent.getPath().equals("/request/enroll/w_id:-1")) {
                System.out.println("Child with -1 found");
            }
            else {
                System.out.println(watchedEvent.getType().toString());
            }
        } }
    }

    public void run() throws InterruptedException, KeeperException {
       try {
           synchronized (this) {
               while (true) {
                   wait();
               }
           }
       } catch (InterruptedException e){
           e.printStackTrace();
           Thread.currentThread().interrupt();
       }
    }


    static CountDownLatch timeout = new CountDownLatch(1);
    public static void main(String[] args) throws IOException, InterruptedException {
        ZooKeeper zoo = new ZooKeeper("localhost", 1000, new Watcher() {
            public void process(WatchedEvent we) {
                if (we.getState() == Event.KeeperState.SyncConnected) {
                    timeout.countDown();
                }
            }
        });
        timeout.await(100, TimeUnit.MILLISECONDS);
        ZooWorker zooWorker = new ZooWorker();
        try {
            zooWorker.run();
            zoo.create("/request/enroll/w_id:-1", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            Thread.sleep(2000);
            while (true){
                System.out.println(zoo.getChildren("/request/enroll", true));
                //zoo.exists("/request/enroll/w_id:-1",  new myWatcher());
                if(zoo.exists("/request/enroll/w_id:-1",  new myWatcher()) != null){
                    System.out.println("-1 child exists");
                }
                break;
            }
            //int version_new = zoo.exists("/master", true).getVersion();
            zoo.delete("/request/enroll/w_id:-1", -1);
            zoo.close();


        }catch (KeeperException e){
            e.printStackTrace();
        }

    }
}
