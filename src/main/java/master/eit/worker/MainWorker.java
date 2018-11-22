package master.eit.worker;

import org.apache.zookeeper.KeeperException;

public class MainWorker {
    public static void main(String[] args) throws KeeperException, InterruptedException  {
        String client_id = "default";
        if (args.length > 0) {
            client_id = args[0];
        }
        ZooWorker zwo = new ZooWorker(client_id);
        Thread zmThread = new Thread(zwo);
        zmThread.setName("zoo worker - " + client_id);
        zmThread.start();
    }
} 