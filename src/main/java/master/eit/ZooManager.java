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

public class ZooManager implements Runnable, Watcher {
    
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

    public void register () throws KeeperException, InterruptedException {
        Stat stat_exist = zoo.exists("/request/enroll", true) ;
        if (stat_exist != null) {
            List<String> children;
            try {
                children = zoo.getChildren("/request/enroll", this);
                for (int i = 0; i < children.size(); i++) {
                    String child = children.get(i);
                    byte[] bdata = zoo.getData("/request/enroll/" + child, true, null);
                    String data = new String(bdata, "UTF-8");
                    if (data.equals(new String("-1"))) {
                        int version_request = zoo.exists("/request/enroll", true).getVersion();
                        try {
                            Stat stat_is_register = zoo.exists("/registry/" + child, true);
                            if (stat_is_register == null) {
                                zoo.create("/registry/" + child, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                                zoo.setData("/request/enroll/" + child, "1".getBytes(), version_request);
                            }
                            else {
                                zoo.setData("/request/enroll/" + child, "2".getBytes(), version_request);                                
                            }
                        } catch (KeeperException e){
                            zoo.setData("/request/enroll/" + child, "0".getBytes(), version_request);
                            e.printStackTrace();
                        }
                    }
                }
            } catch (KeeperException e) {
                e.printStackTrace();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }  
            catch (Exception e ) {
                e.printStackTrace();
            }   
        }
    }

    
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        
        ZooManager zm = new ZooManager();
        Thread thread = new Thread(zm);
        zm.register();
        thread.start();
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