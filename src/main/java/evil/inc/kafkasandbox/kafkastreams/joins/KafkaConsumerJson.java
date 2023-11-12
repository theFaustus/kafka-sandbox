package evil.inc.kafkasandbox.kafkastreams.joins;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Slf4j
public class KafkaConsumerJson {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(GROUP_ID_CONFIG, "join-users-producer-group-id");
        kafkaProps.put(CLIENT_ID_CONFIG, "join-users-producer-client");
        kafkaProps.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        kafkaProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(List.of(BankUserJoinApp.BANK_JOIN_USER_OUTPUT_TOPIC));

        Duration timeout = Duration.ofMillis(100);
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(timeout);
            consumerRecords.forEach(record -> log.info("topic = {}, partition = {}, offset = {}, key = {}, payload = {}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }
}
