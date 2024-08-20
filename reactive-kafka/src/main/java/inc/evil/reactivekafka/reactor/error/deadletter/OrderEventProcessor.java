package inc.evil.reactivekafka.reactor.error.deadletter;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;

@Slf4j
public class OrderEventProcessor {
    private final ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer;

    public OrderEventProcessor(ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer) {
        this.deadLetterTopicProducer = deadLetterTopicProducer;
    }

    public Mono<Void> process(ReceiverRecord<String, String> record) {
        return Mono.just(record)
                .doOnNext(r -> {
                    if (r.key().endsWith("5"))
                        throw new RuntimeException("Processing exception for record -> " + r.key() + ":" + r.value());
                    log.info("r.key: {} and r.value: {}", r.key(), r.value());
                    r.receiverOffset().acknowledge();
                })
                .onErrorMap(ex -> new RecordProcessingException(record, ex))
                .transform(deadLetterTopicProducer.handleProcessingError());
    }

}
