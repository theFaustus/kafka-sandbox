package evil.inc.kafkasandbox.producers;

import evil.inc.kafkasandbox.payload.avro.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
public class KafkaProducerGenericAvro {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        kafkaProps.put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        kafkaProps.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        kafkaProps.put(CLIENT_ID_CONFIG, "KafkaProducerAvroClient");
        kafkaProps.put(ACKS_CONFIG, "all"); //default is 1
        kafkaProps.put(RETRIES_CONFIG, "5");
        kafkaProps.put(COMPRESSION_TYPE_CONFIG, "snappy"); //Snappy compression was invented by Google to provide decent compression ratios with low CPU overhead and good performance
        kafkaProps.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //Must set max.in.flight.requests.per.connection to at most 5 to use the idempotent producer.
        kafkaProps.put(ENABLE_IDEMPOTENCE_CONFIG, "true"); //requires retries > 0 and max.in.flight.requests.per.connection <= 5

        //language=avro
        String schemaString = """
                                {
                  "type": "record",
                  "name": "Customer",
                  "namespace": "evil.inc.kafkasandbox.payload.avro",
                  "fields": [
                    {
                      "name": "id",
                      "type": "int"
                    }
                  ]
                }
                                """;

        int i = 0;
        try (KafkaProducer<String, GenericRecord> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            Schema schema = new Schema.Parser().parse(schemaString);
            while (i <= 100) {
                int random = ThreadLocalRandom.current().nextInt(0, 999);
                GenericRecord customer = new GenericData.Record(schema);
                customer.put("id", random);
                ProducerRecord<String, GenericRecord> customersGenericAvro = new ProducerRecord<>("CustomersGenericAvro", String.valueOf(random), customer);
                kafkaProducer.send(customersGenericAvro, (metadata, exception) -> log.info("Received response {}", metadata));
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
