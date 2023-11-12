alias broker='docker exec -it kafka-cluster'

docker run --rm -it --net=host landoop/fast-data-dev bash

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic bank-account-input

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic bank-account-output
