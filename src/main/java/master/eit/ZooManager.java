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

public class ZooManager {
    static ZooKeeper zoo;

    void setUpTree () throws KeeperException, InterruptedException {
        zoo.create(("/request"), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create(("/request/enroll"), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create(("/request/quit"), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create(("/registry"), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create(("/online"), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    }

    void Register () throws KeeperException, InterruptedException {
        Stat stat = zoo.exists("/request/enroll", true) ;
        if (stat != null) {
            zoo.getChildren("/request/enroll", new RegisterWatcher());
        }
    }


    public static class RegisterWatcher implements Watcher{
        public void process(WatchedEvent we) {
            if (we.getType() == EventType.NodeChildrenChanged) {
                System.out.println("children created");
                List<String> children;
                try {
                    children = zoo.getChildren("/request/enroll", new RegisterWatcher());
                    for (int i = 0; i < children.size(); i++) {
                        String child = children.get(i);
                        String[] w_id = child.split(":");
                        zoo.create("/registry/" + w_id[0],null,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                    } 
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }       
            }
        }
    }

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
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    
        zoo = new ZooKeeper("localhost", 1000, new Watcher(){
            public void process(WatchedEvent we) {
                if (we.getState() == KeeperState.SyncConnected) {
                    timeout.countDown();
                }
            }
        });
        
        timeout.await(100, TimeUnit.MILLISECONDS);
        States state = zoo.getState();
        System.out.println(state);

        ZooManager mng = new ZooManager();
        mng.setUpTree();
        mng.Register();
    }
}