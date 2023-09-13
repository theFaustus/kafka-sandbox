alias broker='docker exec -it broker'

broker kafka-topics --list --bootstrap-server broker:9092

broker kafka-topics --create --bootstrap-server broker:9092 --replication-factor 1 --partitions 1 --topic test-topic

broker kafka-topics --bootstrap-server broker:9092 --topic test-topic --describe

broker kafka-console-producer --bootstrap-server broker:9092 --topic test-topic

broker kafka-console-consumer --bootstrap-server broker:9092 --topic test-topic

broker kafka-topics --create --bootstrap-server broker:9092 --replication-factor 1 --partitions 5 --topic CustomersWithPartitionForMichaelScott

#set up quotas
broker kafka-configs --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024' --entity-name clientC --entity-type clients

broker kafka-configs --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048' --entity-name user1 --entity-type users

broker kafka-configs --bootstrap-server localhost:9092 --alter --add-config 'consumer_byte_rate=2048' --entity-type users
