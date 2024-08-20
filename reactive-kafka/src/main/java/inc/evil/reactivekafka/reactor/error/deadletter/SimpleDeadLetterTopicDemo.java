package inc.evil.reactivekafka.reactor.error.deadletter;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
public class SimpleDeadLetterTopicDemo {
    public static void main(String[] args) {

        ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer = deadLetterTopicProducer();
        OrderEventProcessor orderEventProcessor = new OrderEventProcessor(deadLetterTopicProducer);
        KafkaReceiver<String, String> receiver = kafkaReceiver();

        receiver.receive()
                .log()
                .concatMap(orderEventProcessor::process)
                .subscribe();
    }

    private static ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer() {
        var producerProperties = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        SenderOptions<String, String> senderOptions = SenderOptions.<String, String>create(producerProperties);
        return new ReactiveDeadLetterTopicProducer<>(KafkaSender.create(senderOptions), getRetrySpec());
    }

    private static Retry getRetrySpec() {
        return Retry.fixedDelay(3, Duration.ofSeconds(1))
                .doAfterRetry(r -> log.warn("Retrying #{}", r.totalRetries()));
    }

    public static KafkaReceiver<String, String> kafkaReceiver() {
        var consumerProperties = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-dummy-group",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "dlt-dummy-instance-1",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );

        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(consumerProperties)
                .subscription(List.of("dlt-test-topic"));

        return KafkaReceiver.create(receiverOptions);
    }

}
