package evil.inc.kafkasandbox.reactor.transactions;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
public class TransferEventConsumer {

    private final KafkaReceiver<String, String> receiver;

    public TransferEventConsumer(KafkaReceiver<String, String> receiver) {
        this.receiver = receiver;
    }

    private TransferEvent toTransferEvent(ReceiverRecord<String, String> record) {
        String[] arr = record.value().split(",");
        Runnable runnable = record.key().equals("6") ? fail(record) : ack(record);
        return new TransferEvent(record.key(), arr[0], arr[1], arr[2], runnable);
    }

    public Flux<TransferEvent> receive(){
        return receiver.receive()
                .doOnNext(r -> log.info("Received: {}", r))
                .map(this::toTransferEvent);

    }

    private Runnable ack(ReceiverRecord<String, String> record) {
        return () -> record.receiverOffset().acknowledge();
    }

    private Runnable fail(ReceiverRecord<String, String> record) {
        return () -> { throw new RuntimeException("Error while acknowledging record key: "  + record.key() + ", value: " + record.value()); };
    }
}
