package master.eit.worker;

import org.apache.zookeeper.KeeperException;

public class MainWorker {
    public static void main(String[] args) throws KeeperException, InterruptedException  {
        ZooWorker zwo = new ZooWorker("Ettore&Nicolae");
        Thread zmThread = new Thread(zwo);
        zmThread.setName("zoo worker");
        zmThread.start();
    }
} 