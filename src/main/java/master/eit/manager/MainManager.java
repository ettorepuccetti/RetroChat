package master.eit.manager;

import org.apache.zookeeper.KeeperException;

public class MainManager {
    public static void main(String[] args) throws KeeperException, InterruptedException  {
        ZooManager zm = new ZooManager();
        Thread zmThread = new Thread(zm);
        zmThread.setName("zoo manager");
        zmThread.start();
    }
} 
