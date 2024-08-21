package evil.inc.kafkasandbox.reactor.transactions;

import evil.inc.kafkasandbox.reactor.utils.ReactiveKafkaUtils;
import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;

@Slf4j
public class TransferDemo {

    public static void main(String[] args) {
        KafkaReceiver<String, String> kafkaReceiver = ReactiveKafkaUtils.getStringStringKafkaReceiver("transfer-requests", false);
        KafkaSender<String, String> kafkaSender = ReactiveKafkaUtils.getStringStringKafkaSender(true);

        TransferEventConsumer transferEventConsumer = new TransferEventConsumer(kafkaReceiver);
        TransferEventProcessor transferEventProcessor = new TransferEventProcessor(kafkaSender);

        transferEventConsumer
                .receive()
                .transform(transferEventProcessor::process)
                .doOnNext(r -> log.info("Received event: {}", r.correlationMetadata()))
                .doOnError(ex -> log.error("Error processing event", ex))
                .subscribe();

        ReactiveKafkaUtils.getStringStringKafkaReceiver("transaction-events", true)
                .receive()
                .doOnNext(r -> log.info("Transaction event received r.key: {} and r.value: {} on topic {}", r.key(), r.value(), r.topic()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();

    }


}
