package master.eit.manager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

public class ZooManager implements Runnable {
    
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

        Stat request_exist = zoo.exists("/request", true);
        if ( request_exist != null) {
            Stat enroll_exist = zoo.exists("/request/enroll", true);
            if ( enroll_exist != null) {
                deleteWithChildreen("/request/enroll", enroll_exist.getVersion());
            }
            Stat quit_exist = zoo.exists("/request/quit", true);
            if ( quit_exist != null) {
                deleteWithChildreen("/request/quit", quit_exist.getVersion());
            }
            deleteWithChildreen("/request", request_exist.getVersion());
        }

        Stat registry_exist = zoo.exists("/registry", true);
        if ( registry_exist != null) {
            deleteWithChildreen("/registry", registry_exist.getVersion());
        }

        Stat online_exist = zoo.exists("/online", true);
        if ( online_exist != null) {
            deleteWithChildreen("/online", online_exist.getVersion());
        }
            
        zoo.create("/request", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/request/enroll", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/request/quit", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/registry", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/online", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void deleteWithChildreen (String path, int version) {
        try {
            List<String> children;
            children = zoo.getChildren(path, null);
            for (int i = 0; i < children.size(); i++) {
                String child = children.get(i);
                zoo.delete(path + "/" + child, -1);
            }
            zoo.delete(path, version);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public void register () throws KeeperException, InterruptedException {
        Stat stat_exist = zoo.exists("/request/enroll", true) ;
        if (stat_exist != null) {
            try {
                List<String> children;
                RegisterManagerWatcher rw = new RegisterManagerWatcher(this);
                Thread rwThread = new Thread(rw);
                rwThread.setName("register watcher");
                rwThread.start();
                children = zoo.getChildren("/request/enroll", rw);
                for (int i = 0; i < children.size(); i++) {
                    String child = children.get(i);
                    byte[] bdata = zoo.getData("/request/enroll/" + child, true, null);
                    String data = new String(bdata, "UTF-8");
                    if (data.equals(new String("-1"))) {
                        int version_request = zoo.exists("/request/enroll", true).getVersion();
                        try {
                            Stat stat_registry = zoo.exists("/registry/" + child, true);
                            if (stat_registry == null) {
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
            } catch (Exception e ) {
                e.printStackTrace();
            }   
        }
    }

    public void quit () throws KeeperException, InterruptedException {
        Stat stat_exist = zoo.exists("/request/quit", true) ;
        if (stat_exist != null) {
            try {
                List<String> children;
                QuitManagerWatcher qw = new QuitManagerWatcher(this);
                Thread qwThread = new Thread(qw);
                qwThread.setName("quit watcher");
                qwThread.start();
                children = zoo.getChildren("/request/quit", qw);
                for (int i = 0; i < children.size(); i++) {
                    String child = children.get(i);
                    byte[] bdata = zoo.getData("/request/quit/" + child, true, null);
                    String data = new String(bdata, "UTF-8");
                    if (data.equals("-1")) {
                        int version_request = zoo.exists("/request/quit", true).getVersion();
                        try {
                            Stat stat_registry = zoo.exists("/registry/" + child, true);
                            if (stat_registry != null) {
                                zoo.delete("/registry/" + child, stat_registry.getVersion());
                                zoo.setData("/request/quit/" + child, "1".getBytes(), version_request);
                                List<String> topicsToDelete = new ArrayList<String>();
                                topicsToDelete.add(child);
                                deleteKafkaTopic(topicsToDelete);
                            }
                            else {
                                zoo.setData("/request/quit/" + child, "2".getBytes(), version_request);                                
                            }
                        } catch (KeeperException e){
                            zoo.setData("/request/quit/" + child, "0".getBytes(), version_request);
                            e.printStackTrace();
                        }
                    }
                }
            } catch (Exception e ) {
                e.printStackTrace();
            }   
        }
    }
    public void goOnline() throws KeeperException, InterruptedException {
        Stat stat_exist = zoo.exists("/online", true);
        if (stat_exist != null) {
            try {
                List<String> children;
                OnlineManagerWatcher onlineManagerWatcher = new OnlineManagerWatcher(this);
                Thread onlineManagerThread = new Thread(onlineManagerWatcher);
                onlineManagerThread.setName("online watcher");
                onlineManagerThread.start();
                children = zoo.getChildren("/online", onlineManagerWatcher);
                for (int i = 0; i < children.size(); i++) {
                    String child = children.get(i);
                    try {
                        Stat stat_registry = zoo.exists("/registry/" + child, true);
                        Stat stat_topic = zoo.exists("/brokers/topics/" + child, true);
                        if (stat_registry == null) {
                            System.out.println("CLIENT "+child+" NOT REGISTERED YET...");
                        }
                        if (stat_registry != null && stat_topic == null) {
                            //if topic is not there, it means worker is first time online and we create topic
                            System.out.println("creating topic");
                            KafkaProducer<String, String> kafkaProducer = createProducer();
                            kafkaProducer.send(new ProducerRecord<String, String>(child, "[topic created]"));
                            kafkaProducer.close();
                        }
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static KafkaProducer<String, String> createProducer(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092,localhost:9093,localhost:9094");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    public static void deleteKafkaTopic(Collection<String> topics) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        AdminClient admin = AdminClient.create(config);
        admin.deleteTopics(topics);
    }

    public void run() {
        try {
            register();
            goOnline();
            quit();
            synchronized (this) {
                while(true) {
                    wait();
                }
            }
        } catch (KeeperException e1) {
            e1.printStackTrace();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
}