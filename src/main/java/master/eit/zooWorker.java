package master.eit;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class zooWorker {
    public static class nodeCreator{
        public void create(){
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
        try {
            zoo.create("/request/enroll/w_id:-1", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        }catch (KeeperException e){
            e.printStackTrace();
        }

    }
}
