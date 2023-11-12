alias broker='docker exec -it kafka-cluster'

docker run --rm -it --net=host landoop/fast-data-dev bash

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic favorite-color-input-1

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic favorite-color-output-1

kafka-topics --create --bootstrap-server localhost:9092  --topic favorite-temp-compact-1 \
    --partitions 1 --replication-factor 1 \
    --config cleanup.policy=compact \
    --config min.cleanable.dirty.ratio=0.001 \
    --config segment.ms=5000
