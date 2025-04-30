package evil.inc.kafkasandbox.producers.cloudguru;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@Slf4j
public class KafkaProducerKSQL {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "http://localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //this will cause the producer to receive and acknowledgement for a record only after all in-sync replicas have acknowledged the record.
        kafkaProps.put("acks", "all");

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            for (int i = 0; i < 10; i++) {
                int id = ThreadLocalRandom.current().nextInt(10);
                int vacations = ThreadLocalRandom.current().nextInt(10);
                String name = List.of("mike", "sarah", "john", "bob", "peter", "christen", "alex", "sally", "gregory", "max").get(id);
                ProducerRecord<String, String> record = new ProducerRecord<>("ksql-test-topic", String.valueOf(id), id + "," + name + "," + vacations);
                kafkaProducer.send(record, handleAcknowledgement(record)).get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Callback handleAcknowledgement(ProducerRecord<String, String> record) {
        return (recordMetadata, e) -> {
            if (e != null) {
                log.error("Error in sending record", e);
            } else {
                log.info("Published message={}, topic={}, partition={}", record.value(), recordMetadata.topic(), recordMetadata.partition());
            }
        };
    }
}
