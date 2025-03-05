package evil.inc.kafkasandbox.producers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

@Slf4j
public class KafkaProducerSync {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "http://localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try(KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("CustomerCountry", "Precision Services", "Spain");
            RecordMetadata recordMetadata = kafkaProducer.send(record).get();
            log.info("Received response {}", recordMetadata);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
