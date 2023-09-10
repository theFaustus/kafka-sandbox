package evil.inc.kafkasandbox.producers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class KafkaProducerAdvancedProps {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProps.put("value.serializer", StringSerializer.class.getName());
        kafkaProps.put("client.id", "KafkaProducerAdvancedPropsClient");
        kafkaProps.put("acks", "all"); //default is 1
        kafkaProps.put("retries", "5");
        kafkaProps.put("compression.type", "snappy"); //Snappy compression was invented by Google to provide decent compression ratios with low CPU overhead and good performance
        kafkaProps.put("max.in.flight.requests.per.connection", "5"); //Must set max.in.flight.requests.per.connection to at most 5 to use the idempotent producer.
        kafkaProps.put("enable.idempotence", "true"); //requires retries > 0 and max.in.flight.requests.per.connection <= 5

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            kafkaProducer.send(new ProducerRecord<>("CustomerCountry", "Biomedical Materials", "USA"), (metadata, exception) -> log.info("Received response {}", metadata));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
