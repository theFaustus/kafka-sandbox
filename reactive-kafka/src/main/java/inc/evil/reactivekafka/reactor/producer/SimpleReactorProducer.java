package inc.evil.reactivekafka.reactor.producer;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
public class SimpleReactorProducer {
    public static void main(String[] args) {
        var producerProperties = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        SenderOptions<String, String> senderOptions = SenderOptions.<String, String>create(producerProperties);

        Flux<SenderRecord<String, String, String>> flux = Flux.range(1, 10)
                .map(SimpleReactorProducer::createSenderRecord);

        var kafkaSender = KafkaSender.create(senderOptions);
        kafkaSender
                .send(flux)
                .doOnNext(r -> log.info("result: {}", r.correlationMetadata()))
//                .doOnComplete(kafkaSender::close)
                .subscribe();
    }

    private static SenderRecord<String, String, String> createSenderRecord(Integer i) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("client-id", "123".getBytes());
        headers.add("client-version", "1.0".getBytes());
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test-topic-2", null, i.toString(), "order-" + i, headers);
        return SenderRecord.create(producerRecord, producerRecord.key());
    }
}
