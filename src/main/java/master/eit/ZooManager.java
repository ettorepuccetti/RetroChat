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

public class ZooManager implements Watcher, Runnable {
    
    CountDownLatch timeout = new CountDownLatch(1);
    ZooKeeper zoo;

    public ZooManager () throws KeeperException, InterruptedException {
        
        try {
            zoo = new ZooKeeper("localhost", 1000, new Watcher() {
                public void process(WatchedEvent we) {
                    if (we.getState() == KeeperState.SyncConnected) {
                        timeout.countDown();
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        timeout.await(100, TimeUnit.MILLISECONDS);
        States state = zoo.getState();
        System.out.println(state);

        setUp();
        
    }

    public void setUp() throws KeeperException, InterruptedException {

        zoo.create(("/request"), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create(("/request/enroll"), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create(("/request/quit"), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create(("/registry"), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create(("/online"), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void register () throws KeeperException, InterruptedException {
        Stat stat_exist = zoo.exists("/request/enroll", true) ;
        if (stat_exist != null) {
            List<String> children;
            try {
                children = zoo.getChildren("/request/enroll", this);
                for (int i = 0; i < children.size(); i++) {
                    String child = children.get(i);
                    String[] w_id = child.split(":");
                    System.out.println(child);
                    System.out.println(w_id[0]);
                    Stat stat = zoo.exists("/registry/" + w_id[0], null);
                    if (stat == null) {
                        zoo.create("/registry/" + w_id[0],null,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                        int last_version = zoo.exists("/requerst/enroll/"+child, true).getVersion();
                        zoo.delete("request/enroll" + child, last_version);
                        zoo.create("/request/enroll" + w_id[0] + ":1", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } else {
                        int last_version = zoo.exists("/requerst/enroll/"+child, true).getVersion();
                        zoo.delete("request/enroll" + child, last_version);
                        zoo.create("/request/enroll" + w_id[0] + ":2", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                }
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }       
        }
    }

    public void process(WatchedEvent we) {
        System.out.println("Watcher triggered !!");
        if (we.getType() == EventType.NodeChildrenChanged) {
            try {
                register();
            } catch (KeeperException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } 
    }

    
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        ZooManager zm = new ZooManager();
        zm.register();
        zm.run();
    }

    public void run() {
        try { 
            synchronized (this) {
                while(true) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
}