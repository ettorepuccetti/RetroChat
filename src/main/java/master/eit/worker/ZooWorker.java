package master.eit.worker;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;

import static master.eit.manager.ZooManager.createProducer;

public class ZooWorker implements Runnable{
    
    CountDownLatch timeout = new CountDownLatch(1);
    ZooKeeper zoo;
    String name;

    public ZooWorker (String name) throws KeeperException, InterruptedException {
        
        this.name = name;
        
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
    }

    //why it is creating in ephemeral mode???
    public void register() throws KeeperException, InterruptedException {
        zoo.create("/request/enroll/" + name , "-1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public void quit() throws KeeperException, InterruptedException {
        zoo.create("/request/quit/" + name , "-1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public void watchCreatedNodes () throws KeeperException, InterruptedException, UnsupportedEncodingException {
        RegisterWorkerWatcher rww = new RegisterWorkerWatcher(this);
        Thread rwThread = new Thread(rww);
        rwThread.setName("register worker watcher");
        rwThread.start();
        byte[] bdata = zoo.getData("/request/enroll/" + name, rww, null);
        String data = new String(bdata, "UTF-8");
        if (data.equals("1") || data.equals("2") ) {
            // if I'm here it means the action that I'm watching has already happen,
            // so I do what the watcher is supposed to do, remove the node and release the watcher thread
            removeRequest();
            rww.onetime.countDown();
        }

    }
    public void watchQuittingNodes () throws KeeperException, InterruptedException, UnsupportedEncodingException {
        QuitWorkerWatcher qww = new QuitWorkerWatcher(this);
        Thread qwThread = new Thread(qww);
        qwThread.setName("QuittingNodes worker watcher");
        qwThread.start();
        byte[] bdata = zoo.getData("/request/quit/" + name, qww, null);
        String data = new String(bdata, "UTF-8");
        if (data.equals("1") || data.equals("2") ) {
            // if I'm here it means the action that I'm watching has already happen,
            // so I do what the watcher is supposed to do, remove the node and release the watcher thread
            removeQuit();
            qww.onetime.countDown();
        }

    }

    public void removeRequest () throws KeeperException, InterruptedException {
        int version_delete = zoo.exists("/request/enroll/" + name, true).getVersion();
        zoo.delete("/request/enroll/" + name, version_delete);
    }
    public void removeQuit () throws KeeperException, InterruptedException {
        int version_delete = zoo.exists("/request/quit/" + name, true).getVersion();
        zoo.delete("/request/quit/" + name, version_delete);
    }

    public void createOnlineNode() throws KeeperException, InterruptedException {
        zoo.create("/online/" + name, "-1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public void sendMessage(String w_id, String message) {
        KafkaProducer<String, String> kafkaProducer = createProducer();
        kafkaProducer.send(new ProducerRecord<String, String>(name, w_id, message));
    }

    public void run() {
        try {                
            Scanner in = new Scanner(System.in);
            while (true) {
                String s;
                System.out.println("Enter a string");
                s = in.nextLine();

                int number_code = Integer.parseInt(s);
                System.out.println("Line entered : " + s);
                switch (number_code) {
                    case 1:
                        System.out.println("register");
                        register();
                        watchCreatedNodes();
                        break;
                    case 2:
                        System.out.println("go online");
                        createOnlineNode();
                        break;
                    case 3:
                        System.out.println("quit");
                        quit();
                        watchQuittingNodes();
                        break;
                    case 4:
                        sendMessage("master2018", "Hello");
                        //sendMessage("master2018", "Hello2");
                        break;

                    default:
                        System.out.println("usage: ... bla ...");
                        break;

                    //wait();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}