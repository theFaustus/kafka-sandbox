package evil.inc.kafkasandbox.reactor.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

@Slf4j
public class OneMillionReactorConsumer {
    public static void main(String[] args) {
        var consumerProperties = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "dummy-group",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "dummy-instance-1",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );

        ReceiverOptions<Object, Object> receiverOptions = ReceiverOptions.create(consumerProperties)
                .subscription(List.of("1mln-test-topic-2"));

        KafkaReceiver.create(receiverOptions)
                .receive()
                .doOnNext(r -> log.info("r.key: {} and r.value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();
    }
}
