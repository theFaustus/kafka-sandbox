package evil.inc.kafkasandbox.reactor.error.deadletter;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.retry.Retry;

import java.util.function.Function;

@Slf4j
public class ReactiveDeadLetterTopicProducer<K, V> {

    private final KafkaSender<K, V> sender;
    private final Retry retrySpec;

    public ReactiveDeadLetterTopicProducer(KafkaSender<K, V> sender, Retry retrySpec) {
        this.sender = sender;
        this.retrySpec = retrySpec;
    }

    public Mono<SenderResult<K>> produce(ReceiverRecord<K, V> record) {
        SenderRecord<K, V, K> senderRecord = toSenderRecord(record);
        return this.sender.send(Mono.just(senderRecord))
                .doOnNext(r -> log.info("Dead letter topic correlationMetadata: {}", r.correlationMetadata()))
                .next();
    }

    private SenderRecord<K, V, K> toSenderRecord(ReceiverRecord<K, V> record) {
        String deadLetterTopic = record.topic() + "-dlt";
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(deadLetterTopic, record.key(), record.value());
        log.info("Producing r.key:{} and r.value:{} to dead letter topic {} ", record.key(), record.value(), deadLetterTopic);
        return SenderRecord.create(producerRecord, producerRecord.key());
    }

    public Function<Mono<ReceiverRecord<K, V>>, Mono<Void>> handleProcessingError() {
        return record -> record
                .retryWhen(retrySpec)
                .onErrorMap(ex -> ex.getCause() instanceof RecordProcessingException, Throwable::getCause)
                .doOnError(ex -> log.error("Encountered an exception publishing to dead letter topic {}", ex.getMessage(), ex))
                .onErrorResume(RecordProcessingException.class, ex -> this.produce(ex.getReceiverRecord())
                        .then(Mono.fromRunnable(() -> ex.getReceiverRecord().receiverOffset().acknowledge())))
                .then();
    }
}
