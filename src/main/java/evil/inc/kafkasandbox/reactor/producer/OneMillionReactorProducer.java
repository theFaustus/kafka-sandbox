package evil.inc.kafkasandbox.reactor.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

@Slf4j
public class OneMillionReactorProducer {
    public static void main(String[] args) {
        var producerProperties = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        SenderOptions<String, String> senderOptions = SenderOptions.<String, String>create(producerProperties)
                .maxInFlight(10_000);

        Flux<SenderRecord<String, String, String>> flux = Flux.range(1,  1_000_000)
                .map(i -> new ProducerRecord<>("1mln-test-topic-2", i.toString(), "order-" + i))
                .map(pr -> SenderRecord.create(pr, pr.key()));

        var start = System.currentTimeMillis();
        var kafkaSender = KafkaSender.create(senderOptions);
        kafkaSender
                .send(flux)
                .doOnNext(r -> log.info("Result: {}", r.correlationMetadata()))
                .doOnComplete(() -> {
                    log.info("Total time taken: {} ms", System.currentTimeMillis() - start);
                    kafkaSender.close();
                })
//                .doOnComplete(kafkaSender::close)
                .subscribe();
    }
}
