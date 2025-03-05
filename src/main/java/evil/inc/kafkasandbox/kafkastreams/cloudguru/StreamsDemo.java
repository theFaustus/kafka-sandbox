package evil.inc.kafkasandbox.kafkastreams.cloudguru;

import evil.inc.kafkasandbox.kafkastreams.wordcount.WordCountApp;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static evil.inc.kafkasandbox.kafkastreams.cloudguru.StreamsDemo.STREAMS_INPUT_TOPIC;
import static evil.inc.kafkasandbox.kafkastreams.cloudguru.StreamsDemo.STREAMS_OUTPUT_TOPIC;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

@Slf4j
public class StreamsDemo {
    public static final String STREAMS_INPUT_TOPIC = "streams-input-topic";
    public static final String STREAMS_OUTPUT_TOPIC = "streams-output-topic";

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> source = builder.stream(STREAMS_INPUT_TOPIC);
        source.to(STREAMS_OUTPUT_TOPIC);

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
class StreamsKafkaProducerSimple {


    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            while (true) {
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                kafkaProducer.send(new ProducerRecord<>(STREAMS_INPUT_TOPIC, null, br.readLine()), (metadata, exception) -> log.info("Received response {}", metadata));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

@Slf4j
class StreamsKafkaConsumerSimple {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(GROUP_ID_CONFIG, "simple-consumer");
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

