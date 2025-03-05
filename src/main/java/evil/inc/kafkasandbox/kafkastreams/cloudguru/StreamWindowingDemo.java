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
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static evil.inc.kafkasandbox.kafkastreams.cloudguru.StreamWindowingDemo.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Slf4j
public class StreamWindowingDemo {

    public static final String STREAMS_INPUT_TOPIC = "streams-windowing-input-topic";
    public static final String STREAMS_OUTPUT_TOPIC = "streams-windowing-output-topic";
    public static final String LOCALHOST = "localhost:9092";

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowing-streams-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, LOCALHOST);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> source = builder.stream(STREAMS_INPUT_TOPIC);

        KGroupedStream<String, String> groupedStream = source.groupByKey();
        //advancedBy will make hopping with 2 second gaps, if not specified the advancedBy is by default the time window size
//        TimeWindowedKStream<String, String> windowedStream = groupedStream.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(12)));
        TimeWindowedKStream<String, String> windowedStream = groupedStream.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)));
        KStream<Windowed<String>, String> reduceStream = windowedStream.reduce((aggValue, newValue) -> aggValue + " " + newValue).toStream();
        reduceStream.to(STREAMS_OUTPUT_TOPIC, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));

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
class StreamWindowingKafkaProducerSimple {
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
class StreamWindowingKafkaConsumerSimple {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, LOCALHOST);
        kafkaProps.put(GROUP_ID_CONFIG, "windowing-consumer");
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

