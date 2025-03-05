package evil.inc.kafkasandbox.kafkastreams.cloudguru;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static evil.inc.kafkasandbox.kafkastreams.cloudguru.StreamTransformationsDemo.LOCALHOST;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Slf4j
public class StreamAggregationsDemo {

    public static final String STREAMS_INPUT_TOPIC = "streams-aggregations-input-topic";
    public static final String STREAMS_OUTPUT_TOPIC = "streams-aggregations-output-topic";
    public static final String STREAMS_COUNT_OUTPUT_TOPIC = "streams-aggregations-count-output-topic";
    public static final String STREAMS_REDUCE_OUTPUT_TOPIC = "streams-aggregations-reduce-output-topic";
    public static final String LOCALHOST = "localhost:9092";

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, LOCALHOST);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> source = builder.stream(StreamAggregationsDemo.STREAMS_INPUT_TOPIC);

        //creates an aggregation that totals the length in characters of the value for all records sharing the same key
        KGroupedStream<String, String> groupedStream = source.groupByKey();
        KStream<String, Integer> aggregatedStream = groupedStream.aggregate(() -> 0, ((aggKey, newValue, aggValue) -> aggValue + newValue.length()), Materialized.with(Serdes.String(), Serdes.Integer())).toStream();
        aggregatedStream.to(StreamAggregationsDemo.STREAMS_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));

        //count the number of records for each key.
        KStream<String, Long> countStream = groupedStream.count(Materialized.with(Serdes.String(), Serdes.Long())).toStream();
        countStream.to(StreamAggregationsDemo.STREAMS_COUNT_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        //combine the values of all records with the same key into a string separated by spaces
        KStream<String, String> reducedStream = groupedStream.reduce((aggValue, newValue) -> aggValue + " " + newValue).toStream();
        reducedStream.to(StreamAggregationsDemo.STREAMS_REDUCE_OUTPUT_TOPIC);

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
class StreamAggregationsKafkaProducerSimple {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, LOCALHOST);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            int i = 0;
            while (i < 30) {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                String key = RandomStringUtils.randomAlphabetic(1);
                String value = RandomStringUtils.randomAlphabetic(random.nextInt(50));
                log.info("Sending key={}, value={}", key, value);
                kafkaProducer.send(new ProducerRecord<>(StreamAggregationsDemo.STREAMS_INPUT_TOPIC, key, value), (metadata, exception) -> log.info("Received response {}", metadata));
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

@Slf4j
class StreamAggregationsKafkaConsumerSimple {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, LOCALHOST);
        kafkaProps.put(GROUP_ID_CONFIG, "aggregations-consumer");
        kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(List.of(StreamAggregationsDemo.STREAMS_OUTPUT_TOPIC));

        Duration timeout = Duration.ofMillis(100);
        while (true) {
            ConsumerRecords<String, Long> consumerRecords = consumer.poll(timeout);
            consumerRecords.forEach(record -> log.info("Topic = {}, Partition = {}, Offset = {}, Key = {}, Value = {}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }
}

@Slf4j
class StreamCountKafkaConsumerSimple {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, LOCALHOST);
        kafkaProps.put(GROUP_ID_CONFIG, "aggregations-count-consumer");
        kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(List.of(StreamAggregationsDemo.STREAMS_COUNT_OUTPUT_TOPIC));

        Duration timeout = Duration.ofMillis(100);
        while (true) {
            ConsumerRecords<String, Long> consumerRecords = consumer.poll(timeout);
            consumerRecords.forEach(record -> log.info("Topic = {}, Partition = {}, Offset = {}, Key = {}, Value = {}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }
}

@Slf4j
class StreamReduceKafkaConsumerSimple {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, LOCALHOST);
        kafkaProps.put(GROUP_ID_CONFIG, "aggregations-reduce-consumer");
        kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(List.of(StreamAggregationsDemo.STREAMS_REDUCE_OUTPUT_TOPIC));

        Duration timeout = Duration.ofMillis(100);
        while (true) {
            ConsumerRecords<String, Long> consumerRecords = consumer.poll(timeout);
            consumerRecords.forEach(record -> log.info("Topic = {}, Partition = {}, Offset = {}, Key = {}, Value = {}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }
}

