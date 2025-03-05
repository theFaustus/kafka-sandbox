package evil.inc.kafkasandbox.kafkastreams.cloudguru;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static evil.inc.kafkasandbox.kafkastreams.cloudguru.StreamTransformationsDemo.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Slf4j
public class StreamTransformationsDemo {

    public static final String STREAMS_INPUT_TOPIC = "streams-stateless-input-topic";
    public static final String STREAMS_OUTPUT_TOPIC = "streams-stateless-output-topic";
    public static final String LOCALHOST = "localhost:9092";

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, LOCALHOST);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> source = builder.stream(STREAMS_INPUT_TOPIC);
        Map<String, KStream<String, String>> stringKStreamMap = source.split(Named.as("customers-")).branch((key, value) -> key.startsWith("business")).defaultBranch();
        KStream<String, String> businessCustomers = stringKStreamMap.get("customers-1");
        KStream<String, String> otherCustomers = stringKStreamMap.get("customers-0"); //default is always 0
        KStream<String, String> businessWithdrawTransactions = businessCustomers.filter((key, value) -> value.contains("WITHDRAW"));
        KStream<String, String> businessWithdrawTransactionsWithPrefix = businessWithdrawTransactions.map((key, value) -> KeyValue.pair(key, " -> " + value));
        businessWithdrawTransactionsWithPrefix.peek((key, value) -> log.info("businessWithdrawTransactionsWithPrefix: " + key + " : " + value));
        KStream<String, String> merged = businessWithdrawTransactionsWithPrefix.merge(otherCustomers);

//        businessWithdrawTransactionsWithPrefix.foreach((key, value) -> log.info("businessWithdrawTransactionsWithPrefix: " + key + " : " + value));

        merged.to(STREAMS_OUTPUT_TOPIC);

        final Topology topology = builder.build();
        log.info("Created {}", topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }
}

@Slf4j
class StreamTransformationsKafkaProducerSimple {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, LOCALHOST);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            int i = 0;
            while (i < 30) {
                ThreadLocalRandom current = ThreadLocalRandom.current();
                String key = current.nextBoolean() ? "private-" + i : "business-" + i;
                String value = current.nextBoolean() ? "DEPOSIT : " + i * current.nextInt(50) : "WITHDRAW : " + i * current.nextInt(50);
                kafkaProducer.send(new ProducerRecord<>(STREAMS_INPUT_TOPIC, key, value), (metadata, exception) -> log.info("Received response {}", metadata));
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

@Slf4j
class StreamTransformationsKafkaConsumerSimple {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, LOCALHOST);
        kafkaProps.put(GROUP_ID_CONFIG, "stateless-consumer");
        kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(List.of(STREAMS_OUTPUT_TOPIC));

        Duration timeout = Duration.ofMillis(100);
        while (true) {
            ConsumerRecords<String, Long> consumerRecords = consumer.poll(timeout);
            consumerRecords.forEach(record -> log.info("Topic = {}, Partition = {}, Offset = {}, Key = {}, Value = {}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }
}

