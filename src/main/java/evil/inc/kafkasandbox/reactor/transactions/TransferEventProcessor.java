package evil.inc.kafkasandbox.reactor.transactions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.kafka.sender.TransactionManager;

import java.time.Duration;
import java.util.function.Predicate;

@Slf4j
public class TransferEventProcessor {

    private final KafkaSender<String, String> sender;

    public TransferEventProcessor(KafkaSender<String, String> sender) {
        this.sender = sender;
    }

    public Flux<SenderResult<String>> process(Flux<TransferEvent> flux) {
        return flux
                .concatMap(this::validate)
                .concatMap(this::sendTransaction);
    }


    private Mono<SenderResult<String>> sendTransaction(TransferEvent event) {
        Flux<SenderRecord<String, String, String>> senderRecords = this.toSenderRecords(event);
        TransactionManager transactionManager = this.sender.transactionManager();
        return transactionManager.begin()
                .then(this.sender.send(senderRecords)
                        .concatWith(Mono.delay(Duration.ofSeconds(1)).then(Mono.fromRunnable(event.ack())))
                        .concatWith(transactionManager.commit())
                        .last())
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(ex -> transactionManager.abort());

    }

    private Mono<TransferEvent> validate(TransferEvent transferEvent) {
        return Mono.just(transferEvent)
                .filter(Predicate.not(e -> e.key().equals("5")))
                .switchIfEmpty(Mono.<TransferEvent>fromRunnable(transferEvent.ack()).doFirst(() -> log.info("Failed validation: {}", transferEvent.key())));
    }

    private Flux<SenderRecord<String, String, String>> toSenderRecords(TransferEvent transferEvent) {
        ProducerRecord<String, String> credit = new ProducerRecord<>("transaction-events", transferEvent.key(), "%s+%s".formatted(transferEvent.to(), transferEvent.from()));
        ProducerRecord<String, String> debit = new ProducerRecord<>("transaction-events", transferEvent.key(), "%s-%s".formatted(transferEvent.from(), transferEvent.to()));
        SenderRecord<String, String, String> creditSenderRecord = SenderRecord.create(credit, credit.key());
        SenderRecord<String, String, String> debitSenderRecord = SenderRecord.create(debit, debit.key());
        return Flux.just(creditSenderRecord, debitSenderRecord);
    }
}
