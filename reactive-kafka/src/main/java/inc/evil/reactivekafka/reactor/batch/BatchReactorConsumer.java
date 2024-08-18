package inc.evil.reactivekafka.reactor.batch;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
public class BatchReactorConsumer {
    public static void main(String[] args) {
        var consumerProperties = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "batch-dummy-group",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "batch-dummy-instance-1",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
//                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10 //demo purpose
//                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName() - cooperatively takes partitions from the one with the most, instead of full revoke
        );

        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(consumerProperties)
                .subscription(List.of("batch-test-topic"));

//        KafkaReceiver.create(receiverOptions)
//                .receiveAutoAck()
//                .log()
//                .concatMap(BatchReactorConsumer::processInBatch) //preserves order of emissions
//                .subscribe();

//        KafkaReceiver.create(receiverOptions)
//                .receiveAutoAck()
//                .log()
//                .flatMap(BatchReactorConsumer::processInBatch, 256)  //process concurrently without order of emissions
//                .subscribe();

        KafkaReceiver.<String, String>create(receiverOptions)
                .receive()
                .groupBy(r -> Integer.parseInt(r.key()) % 5) //ensures similar key processing
                .log()
                .flatMap(BatchReactorConsumer::processInBatch, 256)  //process concurrently with order of emissions based on groupBy
                .subscribe();
    }

    private static Mono<Void> processInBatch(Flux<ConsumerRecord<Object, Object>> flux) {
        return flux
//                .publishOn(Schedulers.boundedElastic())  // blocking IO
                .doFirst(() -> log.info("----------"))
                .doOnNext(r -> log.info("r.key: {} and r.value: {}", r.key(), r.value()))
//                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();
    }

    private static Mono<Void> processInBatch(GroupedFlux<Integer, ReceiverRecord<String, String>> flux) {
        return flux
                .publishOn(Schedulers.boundedElastic())  // blocking IO
                .doFirst(() -> log.info("---------- partition key: {}", flux.key()))
                .doOnNext(r -> log.info("r.key: {} and r.value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
//                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();
    }
}
