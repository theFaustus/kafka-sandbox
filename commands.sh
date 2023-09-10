alias broker='docker exec -it broker'

broker kafka-topics --list --bootstrap-server broker:9092

broker kafka-topics --create --bootstrap-server broker:9092 --replication-factor 1 --partitions 1 --topic test-topic

broker kafka-topics --bootstrap-server broker:9092 --topic test-topic --describe

broker kafka-console-producer --bootstrap-server broker:9092 --topic test-topic

broker kafka-console-consumer --bootstrap-server broker:9092 --topic test-topic
