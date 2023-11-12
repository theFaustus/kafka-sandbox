package evil.inc.kafkasandbox.kafkastreams.bankaccount;

import com.github.javafaker.Faker;
import evil.inc.kafkasandbox.payload.json.Customer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
public class KafkaProducerJson {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        kafkaProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());
        kafkaProps.put(CLIENT_ID_CONFIG, "bankaccount-producer");
        kafkaProps.put(ACKS_CONFIG, "all"); //default is 1
        kafkaProps.put(RETRIES_CONFIG, "5");
        kafkaProps.put(COMPRESSION_TYPE_CONFIG, "snappy"); //Snappy compression was invented by Google to provide decent compression ratios with low CPU overhead and good performance
        kafkaProps.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //Must set max.in.flight.requests.per.connection to at most 5 to use the idempotent producer.
        kafkaProps.put(ENABLE_IDEMPOTENCE_CONFIG, "true"); //requires retries > 0 and max.in.flight.requests.per.connection <= 5
        kafkaProps.put(LINGER_MS_CONFIG, "1");

        int j = 0;
        try (KafkaProducer<String, BankTransaction> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            while (j <= 10) {
                int i = 0;
                while (i <= 100) {
                    Faker faker = new Faker();
                    BankTransaction tx = new BankTransaction(faker.name().firstName(), faker.number().randomDouble(2, 1, 999), LocalDateTime.now());
                    kafkaProducer.send(new ProducerRecord<>(BankAccountApp.BANK_ACCOUNT_INPUT_TOPIC, tx.name(), tx), (metadata, exception) -> log.info("Received response {}", metadata));
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
