package evil.inc.kafkasandbox.kafkastreams.favoritecolour;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

@Slf4j
public class KafkaProducerSimple {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            while (true) {
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                kafkaProducer.send(new ProducerRecord<>(FavoriteColorApp.FAV_COLOR_INPUT_TOPIC, null, br.readLine()), (metadata, exception) -> log.info("Received response {}", metadata));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
