package evil.inc.kafkasandbox.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerFireAndForget {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try(KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            kafkaProducer.send(new ProducerRecord<>("CustomerCountry", "Precision Products", "France"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
