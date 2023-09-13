package evil.inc.kafkasandbox.consumers;

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

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Slf4j
public class KafkaConsumerCleanExit {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(GROUP_ID_CONFIG, "KafkaConsumerCleanExitGroup-4");
        kafkaProps.put(CLIENT_ID_CONFIG, "KafkaConsumerCleanExitClient-1");
        kafkaProps.put(GROUP_INSTANCE_ID_CONFIG, "a5f673cf-09c7-4505-b774-c936592e30fd"); //is used to provide a consumer with static group membership.
        /*If you set fetch.max.wait.ms to 100 ms and
        fetch.min.bytes to 1 MB, Kafka will receive a fetch request from the consumer and
        will respond with data either when it has 1 MB of data to return or after 100 ms,
        whichever happens first.
        */
        kafkaProps.put(FETCH_MAX_WAIT_MS_CONFIG, 100);
        kafkaProps.put(FETCH_MIN_BYTES_CONFIG, 1024);
        kafkaProps.put(MAX_POLL_RECORDS_CONFIG, 100);
        kafkaProps.put(SESSION_TIMEOUT_MS_CONFIG, 10000);
        kafkaProps.put(HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        kafkaProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ENABLE_AUTO_COMMIT_CONFIG, false);

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        AtomicInteger count = new AtomicInteger(1);
        Duration timeout = Duration.ofMillis(100);


        final Thread mainThread = Thread.currentThread();


        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps)) {

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Starting exit...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    log.error("Oops", e);
                }
            }));

            consumer.subscribe(List.of("CustomerCountry"), new ConsumerRebalanceListener() {
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
                ConsumerRecords<String, String> consumerRecords = consumer.poll(timeout);
                consumerRecords.forEach(record -> {
                    log.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
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
