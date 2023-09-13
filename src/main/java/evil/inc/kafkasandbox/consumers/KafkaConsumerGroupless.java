package evil.inc.kafkasandbox.consumers;

import evil.inc.kafkasandbox.payload.json.Customer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Slf4j
public class KafkaConsumerGroupless {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(GROUP_ID_CONFIG, "KafkaConsumerGroupless-5"); //although you still need to configure group.id to commit offsets, without calling subscribe the consumer won’t join any group
        kafkaProps.put(CLIENT_ID_CONFIG, "KafkaConsumerGroupless-1");
        kafkaProps.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        kafkaProps.put(FETCH_MAX_WAIT_MS_CONFIG, 100);
        kafkaProps.put(FETCH_MIN_BYTES_CONFIG, 1024);
        kafkaProps.put(MAX_POLL_RECORDS_CONFIG, 100);
        kafkaProps.put(SESSION_TIMEOUT_MS_CONFIG, 10000);
        kafkaProps.put(HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        kafkaProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getName());
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

            List<PartitionInfo> partitionInfoList = consumer.partitionsFor("CustomersJson");
            List<TopicPartition> partitions = partitionInfoList.stream()
                    .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                    .collect(Collectors.toList());

            consumer.assign(partitions);

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
