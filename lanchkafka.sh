zkServer.sh start myconfig.cfg
sleep 1
cd kafka/
bin/kafka-server-start.sh config/server-0.properties &
sleep 1
bin/kafka-server-start.sh config/server-1.properties &
sleep 1
bin/kafka-server-start.sh config/server-2.properties &
