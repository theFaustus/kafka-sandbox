package evil.inc.kafkasandbox.consumers;

import evil.inc.kafkasandbox.payload.avro.Customer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Slf4j
public class KafkaConsumerAvro {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(GROUP_ID_CONFIG, "KafkaConsumerAvro-5");
        kafkaProps.put(CLIENT_ID_CONFIG, "KafkaConsumerAvro-1");
        kafkaProps.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        kafkaProps.put(FETCH_MAX_WAIT_MS_CONFIG, 100);
        kafkaProps.put(FETCH_MIN_BYTES_CONFIG, 1024);
        kafkaProps.put(MAX_POLL_RECORDS_CONFIG, 100);
        kafkaProps.put(SESSION_TIMEOUT_MS_CONFIG, 10000);
        kafkaProps.put(HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        kafkaProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        kafkaProps.put(ENABLE_AUTO_COMMIT_CONFIG, false);

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        AtomicInteger count = new AtomicInteger(1);
        Duration timeout = Duration.ofMillis(100);


        final Thread mainThread = Thread.currentThread();


        try (KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(kafkaProps)) {

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Starting exit...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    log.error("Oops", e);
                }
            }));

            consumer.subscribe(List.of("CustomersAvroWithHeaders"), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    log.info("Lost partitions in rebalance. Committing current offsets {}", currentOffsets);
                    consumer.commitSync(currentOffsets);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

                }
            });

            while (true) {
                ConsumerRecords<String, Customer> consumerRecords = consumer.poll(timeout);
                consumerRecords.forEach(record -> {
                    log.info("topic = {}, partition = {}, offset = {}, customer id = {}, customer = {}, headers = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value(), record.headers());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, record.headers().toString()));
                    if (count.get() % 5 == 0) {
                        log.info("Committing offsets {}", currentOffsets);
                        consumer.commitAsync(currentOffsets, (offsets, e) -> {
                            if (e != null) log.error("Commit failed for offsets {}", offsets, e);
                        });
                    }
                    count.getAndIncrement();

                });
            }
        } catch (Exception e) {
            log.error("Unexpected error", e);
        }
    }
}
