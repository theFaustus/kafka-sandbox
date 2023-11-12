package evil.inc.kafkasandbox.kafkastreams.joins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

@Slf4j
public class BankUserJoinApp {
    public static final String BANK_ACCOUNT_INPUT_TOPIC = "bank-account-input-1";
    public static final String USER_INPUT_TOPIC = "user-input-1";
    public static final String BANK_JOIN_USER_OUTPUT_TOPIC = "bank-join-user-output-1";

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(APPLICATION_ID_CONFIG, "bank-join-user-application");
        config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        config.put(CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        String initialBalance;
        try {
            initialBalance = objectMapper.writeValueAsString(new BankBalance(0, 0.0, LocalDateTime.MIN));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        StreamsBuilder builder = new StreamsBuilder();
        GlobalKTable<String, String> usersGlobalKTable = builder.globalTable(USER_INPUT_TOPIC);

        builder.<String, String>stream(BANK_ACCOUNT_INPUT_TOPIC)
                .groupByKey()
                .aggregate(() -> initialBalance, (key, value, aggregate) -> newBalance(value, aggregate, objectMapper))
                .toStream()
                .leftJoin(usersGlobalKTable, (key, value) -> key, (value1, value2) -> "Transaction=" + value1 + ", User=" + (value2 != null ? value2 : "n/a"))
                .to(BANK_JOIN_USER_OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();
        log.info("Printing the bank-account stream {}", streams.state());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static String newBalance(String tx, String balance, ObjectMapper objectMapper) {
        try {
            BankBalance bankBalance = objectMapper.readValue(balance, BankBalance.class);
            BankTransaction bankTransaction = objectMapper.readValue(tx, BankTransaction.class);
            BankBalance newBankBalance = new BankBalance(
                    bankBalance.count() + 1,
                    bankBalance.balance() + bankTransaction.amount(),
                    Collections.max(List.of(bankTransaction.time(), bankBalance.time())));
            return objectMapper.writeValueAsString(newBankBalance);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
