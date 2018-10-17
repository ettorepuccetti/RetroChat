package master.eit;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

public class testConnection {

    public static class myWatcher implements Watcher{
        public void process(WatchedEvent we) {
            if (we.getType() == EventType.NodeChildrenChanged) {
                System.out.println("children changed");
            }
            else {
                System.out.println(we.getType().toString());
            }
        }
    }
    
    static CountDownLatch timeout = new CountDownLatch(1);
    public static void main(String[] args) throws IOException, InterruptedException {
       
        ZooKeeper zoo = new ZooKeeper("localhost", 1000, new Watcher() {
            public void process(WatchedEvent we) {
                if (we.getState() == KeeperState.SyncConnected) {
                    timeout.countDown();
                }
            }
        });

        timeout.await(100, TimeUnit.MILLISECONDS);
        States state = zoo.getState();
        System.out.println(state);

        try {
            zoo.create(("/master"), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(zoo.exists("/master/subnode1", new myWatcher()));
            zoo.create(("/master/subnode1"), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Stat stat = zoo.exists("/master", false);
            System.out.println(stat);
            int version = zoo.exists("/master", true).getVersion();
            zoo.setData("/master", "my data".getBytes(), version);
            byte[] data = zoo.getData("/master", new myWatcher(), null);
            String data_str = new String(data, "UTF-8");
            System.out.println(data_str);
            int version_new = zoo.exists("/master", true).getVersion();
            List<String> child = zoo.getChildren("/master", new myWatcher());
            System.out.println(child);
            zoo.delete("/master/subnode1", -1);
            zoo.delete("/master", version_new);
            zoo.close();
        } catch (KeeperException e) {
            e.printStackTrace();
            try {
                zoo.delete("/master", -1);
            } catch (KeeperException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            zoo.close();
        } catch (InterruptedException e) {
            zoo.close();
            return;
        }
    }
}