package evil.inc.kafkasandbox.kafkastreams.joins;

import com.github.javafaker.Faker;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
public class KafkaProducerUserJson {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        kafkaProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());
        kafkaProps.put(CLIENT_ID_CONFIG, "user-producer");
        kafkaProps.put(ACKS_CONFIG, "all"); //default is 1
        kafkaProps.put(RETRIES_CONFIG, "5");
        kafkaProps.put(COMPRESSION_TYPE_CONFIG, "snappy"); //Snappy compression was invented by Google to provide decent compression ratios with low CPU overhead and good performance
        kafkaProps.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //Must set max.in.flight.requests.per.connection to at most 5 to use the idempotent producer.
        kafkaProps.put(ENABLE_IDEMPOTENCE_CONFIG, "true"); //requires retries > 0 and max.in.flight.requests.per.connection <= 5
        kafkaProps.put(LINGER_MS_CONFIG, "1");

        int j = 0;
        try (KafkaProducer<String, User> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            while (j <= 10) {
                int i = 0;
                while (i <= 100) {
                    Faker faker = new Faker();
                    User user = new User(faker.name().firstName(), faker.name().lastName(), faker.number().randomNumber(2, true));
                    kafkaProducer.send(new ProducerRecord<>(BankUserJoinApp.USER_INPUT_TOPIC, user.firstName(), user), (metadata, exception) -> log.info("Received response {}", metadata));
                    i++;
                }
                Thread.sleep(1000);
                j++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
