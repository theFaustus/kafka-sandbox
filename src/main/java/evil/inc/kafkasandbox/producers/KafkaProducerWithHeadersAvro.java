package evil.inc.kafkasandbox.producers;

import evil.inc.kafkasandbox.payload.avro.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class KafkaProducerWithHeadersAvro {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        kafkaProps.put("key.serializer", KafkaAvroSerializer.class.getName());
        kafkaProps.put("value.serializer", KafkaAvroSerializer.class.getName());
        kafkaProps.put("client.id", "KafkaProducerAvroClient");
        kafkaProps.put("acks", "all"); //default is 1
        kafkaProps.put("retries", "5");
        kafkaProps.put("compression.type", "snappy"); //Snappy compression was invented by Google to provide decent compression ratios with low CPU overhead and good performance
        kafkaProps.put("max.in.flight.requests.per.connection", "5"); //Must set max.in.flight.requests.per.connection to at most 5 to use the idempotent producer.
        kafkaProps.put("enable.idempotence", "true"); //requires retries > 0 and max.in.flight.requests.per.connection <= 5

        int i = 0;
        try (KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            while (i <= 100) {
                int random = ThreadLocalRandom.current().nextInt(0, 999);
                Customer customer = new Customer(random, "Mike-" + random, "42313" + random);
                ProducerRecord<String, Customer> record = new ProducerRecord<>("CustomersAvroWithHeaders", String.valueOf(random), customer);
                record.headers().add("privacy-level", "YOLO".getBytes(StandardCharsets.UTF_8));
                record.headers().add("trace-id", "51e82923-605d-417c-a6f5-07718824d0e0".getBytes(StandardCharsets.UTF_8));
                kafkaProducer.send(record, (metadata, exception) -> log.info("Received response {}", metadata));
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
