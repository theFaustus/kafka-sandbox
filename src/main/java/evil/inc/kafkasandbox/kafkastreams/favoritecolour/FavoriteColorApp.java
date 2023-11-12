package evil.inc.kafkasandbox.kafkastreams.favoritecolour;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class FavoriteColorApp {
    public static final String FAV_COLOR_INPUT_TOPIC = "favorite-color-input-1";
    public static final String FAV_COLOR_TEMP_TOPIC = "favorite-temp-compact-1";
    public static final String FAV_COLOR_OUTPUT_TOPIC = "favorite-color-output-1";

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(FAV_COLOR_INPUT_TOPIC)
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues((readOnlyKey, value) -> value.split(",")[1].toLowerCase())
                .filter((key, value) -> Arrays.asList("blue", "green", "red").contains(value))
                .to(FAV_COLOR_TEMP_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        builder.<String, String>table(FAV_COLOR_TEMP_TOPIC)
                .groupBy((key, value) -> new KeyValue<>(value, value))
                .count(Named.as("counts"))
                .toStream()
                .to(FAV_COLOR_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
        log.info("Printing the fav-color stream {}", streams.state());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
