package master.eit.worker;

import org.apache.zookeeper.KeeperException;

public class MainWorker {
    public static void main(String[] args) throws KeeperException, InterruptedException  {
        ZooWorker zwo = new ZooWorker("EttoreNicolae"); // it doesn't work with &
        Thread zmThread = new Thread(zwo);
        zmThread.setName("zoo worker");
        zmThread.start();
    }
} 