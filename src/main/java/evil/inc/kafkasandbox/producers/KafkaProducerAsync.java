package evil.inc.kafkasandbox.producers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

@Slf4j
public class KafkaProducerAsync {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            kafkaProducer.send(new ProducerRecord<>("CustomerCountry", "Biomedical Materials", "USA"), (metadata, exception) -> log.info("Received response {}", metadata));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
