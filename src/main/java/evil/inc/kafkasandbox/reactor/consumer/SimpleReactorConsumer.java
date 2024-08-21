package evil.inc.kafkasandbox.reactor.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

@Slf4j
public class SimpleReactorConsumer {
    public static void main(String[] args) {
        var consumerProperties = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "dummy-group",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "dummy-instance-1",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
//                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName() - cooperatively takes partitions from the one with the most, instead of full revoke
        );

        ReceiverOptions<Object, Object> receiverOptions = ReceiverOptions.create(consumerProperties)
                .addAssignListener(c -> {
                    //get last 2 items from every partition
                    c.forEach(r -> r.seek(r.position() - 2));
                    //get last 2 items from partition 2
                    c.stream().filter(r -> r.topicPartition().partition() == 2).findFirst().ifPresent(r -> r.seek(r.position() - 2));

                })
                .subscription(List.of("test-topic-2"));

        KafkaReceiver.create(receiverOptions)
                .receive()
                .doOnNext(r -> log.info("r.key: {} and r.value: {}", r.key(), r.value()))
                .doOnNext(r -> r.headers().forEach(header -> log.info("r.header.key: {}, r.header.value: {}", header.key(), new String(header.value()))))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();
    }
}
