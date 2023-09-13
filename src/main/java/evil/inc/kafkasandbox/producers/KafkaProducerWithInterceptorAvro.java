package evil.inc.kafkasandbox.producers;

import evil.inc.kafkasandbox.payload.avro.Customer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
public class KafkaProducerWithInterceptorAvro {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        kafkaProps.put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        kafkaProps.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        kafkaProps.put(CLIENT_ID_CONFIG, "KafkaProducerAvroClient");
        kafkaProps.put(ACKS_CONFIG, "all"); //default is 1
        kafkaProps.put(RETRIES_CONFIG, "5");
        kafkaProps.put(COMPRESSION_TYPE_CONFIG, "snappy"); //Snappy compression was invented by Google to provide decent compression ratios with low CPU overhead and good performance
        kafkaProps.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //Must set max.in.flight.requests.per.connection to at most 5 to use the idempotent producer.
        kafkaProps.put(ENABLE_IDEMPOTENCE_CONFIG, "true"); //requires retries > 0 and max.in.flight.requests.per.connection <= 5
        kafkaProps.put(INTERCEPTOR_CLASSES_CONFIG, "evil.inc.kafkasandbox.producers.interceptor.CountingProducerInterceptor");
        kafkaProps.put("counting.interceptor.window.size.ms", "100");

        int i = 0;
        try (KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            while (i <= 10000) {
                int random = ThreadLocalRandom.current().nextInt(0, 999);
                Customer customer = new Customer(random, "Mike-" + random, "42313" + random);
                ProducerRecord<String, Customer> record = new ProducerRecord<>("CustomersWithInterceptorAvro", String.valueOf(random), customer);
                kafkaProducer.send(record, (metadata, exception) -> log.info("Received response {}", metadata));
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
