package inc.evil.reactivekafka.reactor.transactions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class TransferRequestProducer {
    public static void main(String[] args) {
        var producerProperties = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        SenderOptions<String, String> senderOptions = SenderOptions.<String, String>create(producerProperties);

        Flux<SenderRecord<String, String, String>> flux = Flux.range(1, 100)
                .delayElements(Duration.ofSeconds(3))
                .map(TransferRequestProducer::createSenderRecord);

        var kafkaSender = KafkaSender.create(senderOptions);
        kafkaSender
                .send(flux)
                .doOnNext(r -> log.info("CorrelationMetadata: {}", r.correlationMetadata()))
                .doOnComplete(kafkaSender::close)
                .subscribe();
    }

    private static SenderRecord<String, String, String> createSenderRecord(Integer i) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("client-id", "123".getBytes());
        headers.add("client-version", "1.0".getBytes());
        String[] accounts = new String[]{"a", "b", "c", "d", "e", "f", "g", "h"};
        String firstAccount = accounts[ThreadLocalRandom.current().nextInt(accounts.length)];
        String secondAccount = accounts[ThreadLocalRandom.current().nextInt(accounts.length)];
        int amount = ThreadLocalRandom.current().nextInt(100);
        String value = "%s,%s,%s".formatted(firstAccount, secondAccount, amount);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("transfer-requests", null, String.valueOf(ThreadLocalRandom.current().nextInt(10)), value, headers);
        log.info("Producing transfer-request: r.key: {} and r.value: {}", producerRecord.key(), producerRecord.value());
        return SenderRecord.create(producerRecord, producerRecord.key());
    }
}
