package inc.evil.reactivekafka.reactor.error;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class SimpleReactorConsumer {
    public static void main(String[] args) {
        var consumerProperties = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "error-dummy-group",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "error-dummy-instance-1",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );

        ReceiverOptions<Object, Object> receiverOptions = ReceiverOptions.create(consumerProperties)
                .subscription(List.of("error-test-topic"));

        KafkaReceiver.create(receiverOptions)
                .receive()
                .log()
                .concatMap(SimpleReactorConsumer::handle)
                .subscribe();
    }

    private static Mono<Void> handle(ReceiverRecord<Object, Object> record) {
        return Mono.just(record)
                .doOnNext(r -> {
                    int index = ThreadLocalRandom.current().nextInt(1, 10);
                    log.info("r.key: {} and index: {} r.value: {}", r.key(), index, r.value().toString().toCharArray()[index]);
                })
                .retryWhen(getRetrySpec())
                .doOnError(ex -> log.error(ex.getMessage(), ex))
                .doFinally(s -> record.receiverOffset().acknowledge())
                .onErrorComplete()
                .then();

    }

    private static Retry getRetrySpec() {
        return Retry.fixedDelay(3, Duration.ofSeconds(1))
                .filter(IndexOutOfBoundsException.class::isInstance)
                .doAfterRetry(r -> log.warn("Retrying #{}", r.totalRetries()))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());

    }
}
