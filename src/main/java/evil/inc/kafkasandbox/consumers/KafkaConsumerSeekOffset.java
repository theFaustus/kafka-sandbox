package evil.inc.kafkasandbox.consumers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Slf4j
public class KafkaConsumerSeekOffset {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(GROUP_ID_CONFIG, "KafkaConsumerSeekOffsetGroup-7");
        kafkaProps.put(CLIENT_ID_CONFIG, "KafkaConsumerSeekOffsetClient-1");
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

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps)) {
            consumer.subscribe(List.of("CustomerCountry"));
            long tenMinutesEarlier = Instant.now().atZone(ZoneId.systemDefault()).minusMinutes(10).toEpochSecond();
            Map<TopicPartition, Long> partitionTimestamps = consumer.assignment()
                    .stream()
                    .collect(Collectors.toMap(topicPartition -> topicPartition, topicPartition -> tenMinutesEarlier));
            Map<TopicPartition, OffsetAndTimestamp> offsetMap = consumer.offsetsForTimes(partitionTimestamps);
            offsetMap.forEach((k, v) -> consumer.seek(k, v.offset()));
//            consumer.seekToBeginning(consumer.assignment());
//            consumer.seekToEnd(consumer.assignment());
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                consumerRecords.forEach(record -> log.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                //with enable.auto.commit = false, consumer will process the same messages infinitely if no commit occurs
                consumer.commitAsync((offsets, e) -> {
                    if (e != null) log.error("Commit failed for offsets {}", offsets, e);
                });
            }

        } catch (Exception e) {
            log.error("Unexpected error", e);
        }
    }
}
