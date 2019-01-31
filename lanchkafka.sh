zkServer.sh start myconfig.cfg
sleep 1
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server-0.properties &
sleep 1
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server-1.properties &
sleep 1
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server-2.properties &
