package evil.inc.kafkasandbox.producers;

import evil.inc.kafkasandbox.payload.avro.Client;
import evil.inc.kafkasandbox.payload.avro.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class KafkaProducerAvroCompatibility {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        kafkaProps.put("key.serializer", KafkaAvroSerializer.class.getName());
        kafkaProps.put("value.serializer", KafkaAvroSerializer.class.getName());
        kafkaProps.put("client.id", "KafkaProducerAvroClientCompatibility");
        kafkaProps.put("acks", "all"); //default is 1
        kafkaProps.put("retries", "5");
        kafkaProps.put("compression.type", "snappy"); //Snappy compression was invented by Google to provide decent compression ratios with low CPU overhead and good performance
        kafkaProps.put("max.in.flight.requests.per.connection", "5"); //Must set max.in.flight.requests.per.connection to at most 5 to use the idempotent producer.
        kafkaProps.put("enable.idempotence", "true"); //requires retries > 0 and max.in.flight.requests.per.connection <= 5

        int i = 0;
        try (KafkaProducer<String, Client> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            while (i <= 10) {
                int random = ThreadLocalRandom.current().nextInt(0, 999);
//              Testing backward compatibility - Schema being registered is incompatible with an earlier schema for subject
//              Client customer = new Client(random, "Mike-" + random, "mike-" + random + "@mail.com", "@Mike" + random);
                Client customer = new Client(random, random, "@Mike" + random);
                ProducerRecord<String, Client> record = new ProducerRecord<>("ClientsAvro", String.valueOf(random), customer);
                kafkaProducer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Oops, something happened", exception);
                    } else {
                        log.info("Received response {}", metadata);
                    }
                });
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
