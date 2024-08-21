package inc.evil.reactivekafka.reactor.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ReactiveKafkaUtils {

    public static KafkaReceiver<String, String> getStringStringKafkaReceiver(String topic, Boolean isTransactional) {
        var consumerProperties = new java.util.HashMap<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, topic + "-dummy-group",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, topic + "-instance-id-1",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ));

        if (isTransactional) {
            consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }

        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(consumerProperties)
                .subscription(List.of(topic));

        return KafkaReceiver.create(receiverOptions);
    }


    public static KafkaSender<String, String> getStringStringKafkaSender(Boolean isTransactional) {
        var producerProperties = new java.util.HashMap<>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        ));

        if (isTransactional)
            producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "money-transfers-" + UUID.randomUUID());

        SenderOptions<String, String> senderOptions = SenderOptions.<String, String>create(producerProperties);
        return KafkaSender.create(senderOptions);
    }
}
