package evil.inc.kafkasandbox.producers;

import evil.inc.kafkasandbox.payload.avro.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class KafkaProducerAvro {
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

        try (KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            while (true) {
                int random = ThreadLocalRandom.current().nextInt(0, 999);
                Customer customer = new Customer(random, "Mike-" + random, "42313" + random);
                kafkaProducer.send(new ProducerRecord<>("Customers", String.valueOf(random), customer), (metadata, exception) -> log.info("Received response {}", metadata));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
