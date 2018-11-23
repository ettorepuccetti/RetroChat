package master.eit.worker;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import static master.eit.manager.ZooManager.createProducer;

public class ZooWorker implements Runnable{
    
    CountDownLatch timeout = new CountDownLatch(1);
    ZooKeeper zoo;
    String name;
    Scanner messageScanner;

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

    public void register() throws KeeperException, InterruptedException {
        zoo.create("/request/enroll/" + name , "-1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void quit() throws KeeperException, InterruptedException {
        zoo.create("/request/quit/" + name , "-1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void watchRegisterNodes () throws KeeperException, InterruptedException, UnsupportedEncodingException {
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
            // if I'm here it means the action that I'm watching has already happened,
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
        Stat online = zoo.exists("/online/"+name, null);
        if (online == null) {
            zoo.create("/online/" + name, "-1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            System.out.println("CLIENT "+name+" ALREADY ONLINE !");
        }
    }

    
    public void printTopic () throws InterruptedException {
        try {
            List<String> topics = zoo.getChildren("/brokers/topics", true);
            Iterator<String> topicsIterator = topics.iterator();
            System.out.println("\nONLINE CLIENTS:");
            while (topicsIterator.hasNext()) {
                String topic = topicsIterator.next();
                if (!topic.equals("__consumer_offsets")) System.out.println("* " + topic);
            }
            System.out.println("");
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public void sendMessages () throws InterruptedException {
        this.messageScanner = new Scanner(System.in);
        System.out.println("\n Choose the USER. Enter \"--list\" for viewing online users ");
        String topic = messageScanner.nextLine();
        while (topic.equals("--list")) {
            // calling function to print online users
            printTopic();
            topic = messageScanner.nextLine();
        }

        // todo: check if the selected user is online

        System.out.println("\nEnter MESSAGEs to " + topic + ". Enter \"--quit\" for closing ");
        String message = messageScanner.nextLine();
        KafkaProducer<String, String> kafkaProducer = createProducer();
        while (!message.equals("--quit")) {
            kafkaProducer.send(new ProducerRecord<String, String>(topic, message));
            message = messageScanner.nextLine();
        }
        kafkaProducer.close();
    }

    public void readMessages () {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", name);
        props.put("enable.auto.commit", "false");
        // props.put("auto.commit.interval.ms","1000");
        // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        try {
            consumer.subscribe(Collections.singletonList(name));
            consumer.poll(0);
            consumer.seekToBeginning(consumer.assignment());
            ConsumerRecords<String, String> records = consumer.poll(1000);
            System.out.println("\n INBOX FOR "+name+" :");
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("> "+record.value());
            }
        }
        finally { consumer.close();}
    }


    public void run() {
        try {
            int number_code = 99; // so if it initially fail to scan the value from System.in, it loop on the while and it try again.
            String s;
            Scanner in = new Scanner(System.in);
            
            System.out.println("\nusage:\n 1 - register \n 2 - go online \n 3 - quit \n 4 - send messages");
            System.out.println(" 5 - watch online clients \n 6 - read my messages \n 0 - exit\n");            
            try {
                s = in.nextLine();
                number_code = Integer.parseInt(s); 
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (NoSuchElementException e) {
                e.printStackTrace();
            }

            while (number_code != 0) {
                switch (number_code) {
                    case 1:
                        System.out.println("\nREGISTER");
                        register();
                        watchRegisterNodes();
                        break;
                    case 2:
                        System.out.println("\nGO ONLINE");
                        createOnlineNode();
                        break;
                    case 3:
                        System.out.println("\nQUIT");
                        quit();
                        watchQuittingNodes();
                        break;
                    case 4:
                        System.out.println("\nSENDING MESSAGES");
                        sendMessages();
                        break;
                    case 5:
                        printTopic();
                        break;
                    case 6:
                        readMessages();
                        break;
                    default:
                        break;
                }
                System.out.println("\nusage:\n 1 - register \n 2 - go online \n 3 - quit \n 4 - send messages");
                System.out.println(" 5 - watch online clients \n 6 - read my messages \n 0 - exit\n");
                try {
                    s = in.nextLine();
                    number_code = Integer.parseInt(s); 
                } catch (NumberFormatException e) {
                    number_code = 99;
                    continue;
                } catch (NoSuchElementException e) {
                    e.printStackTrace();
                    continue;
                }
            }
            in.close();
            messageScanner.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
