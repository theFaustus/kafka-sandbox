alias broker='docker exec -it broker'

docker run --rm -it --net=host landoop/fast-data-dev bash

docker exec -it kafka-cluster bash

kafka-topics --list --bootstrap-server localhost:9092

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic test-topic

kafka-topics --create --bootstrap-server localhost:9092 --topic transfer-events

kafka-topics --create --bootstrap-server localhost:9092 --topic transaction-events

kafka-topics --bootstrap-server localhost:9092 --topic test-topic --describe

kafka-consumer-groups \
--bootstrap-server localhost:9092 \
--group ps \
--topic test-topic \
--reset-offsets \
--shift-by -10 \
--execute
#--dry-run - for simulation

kafka-consumer-groups \
--bootstrap-server localhost:9092 \
--group ps \
--describe

kafka-console-producer --bootstrap-server localhost:9092 --topic test-topic

#key is needed for murmur2 algorithm to decide to which partition to send
kafka-console-producer \
--bootstrap-server localhost:9092 \
--topic test-topic \
--property key.separator=: \
--property parse.key=true

kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic

kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic test-topic \
--from-beginning \
--property print.key=true \
--property print.offset=true \
--property print.timestamp=true  \
--group ps

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 5 --topic CustomersWithPartitionForMichaelScott

#set up quotas
kafka-configs --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024' --entity-name clientC --entity-type clients

kafka-configs --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048' --entity-name user1 --entity-type users

kafka-configs --bootstrap-server localhost:9092 --alter --add-config 'consumer_byte_rate=2048' --entity-type users

#compaction log
kafka-topics --create --bootstrap-server localhost:9092  --topic employee-salary \
    --partitions 1 --replication-factor 1 \
    --config cleanup.policy=compact \
    --config min.cleanable.dirty.ratio=0.001 \
    --config segment.ms=5000

kafka-topics --describe --bootstrap-server localhost:9092 --topic employee-salary

kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic employee-salary \
    --from-beginning \
    --property print.key=true \
    --property key.separator=,

kafka-console-producer --bootstrap-server localhost:9092 \
    --topic employee-salary \
    --property parse.key=true \
    --property key.separator=,

#To produce
#Patrick,salary: 10000
#Lucy,salary: 20000
#Bob,salary: 20000
#Patrick,salary: 25000
#Lucy,salary: 30000
#Patrick,salary: 30000
