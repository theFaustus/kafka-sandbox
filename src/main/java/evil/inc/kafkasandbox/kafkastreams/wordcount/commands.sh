alias broker='docker exec -it kafka-cluster'

docker run --rm -it --net=host landoop/fast-data-dev bash

$ broker kafka-topics --create --bootstrap-server kafka-cluster:9092 --replication-factor 1 --partitions 2 --topic word-count-input

$ broker kafka-topics --create --bootstrap-server kafka-cluster:9092 --replication-factor 1 --partitions 2 --topic word-count-output
