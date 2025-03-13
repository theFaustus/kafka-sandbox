package evil.inc.kafkasandbox.producers.cloudguru;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;

@Slf4j
public class KafkaProducerSimple {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "http://localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //this will cause the producer to receive and acknowledgement for a record only after all in-sync replicas have acknowledged the record.
        kafkaProps.put("acks", "all");

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            for (int i = 0; i < 100; i++) {
                int partition = 0;
                if (i > 49) partition = 1; //custom partition check
                ProducerRecord<String, String> record = new ProducerRecord<>("test_count_x", partition, "count", Integer.toString(i));
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
