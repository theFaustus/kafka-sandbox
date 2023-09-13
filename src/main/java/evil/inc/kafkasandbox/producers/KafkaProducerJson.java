package evil.inc.kafkasandbox.producers;

import evil.inc.kafkasandbox.payload.json.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class KafkaProducerJson {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        kafkaProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProps.put("value.serializer", KafkaJsonSerializer.class.getName());
        kafkaProps.put("client.id", "KafkaProducerJsonClient");
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
                kafkaProducer.send(new ProducerRecord<>("CustomersJson", String.valueOf(random), customer), (metadata, exception) -> log.info("Received response {}", metadata));
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
