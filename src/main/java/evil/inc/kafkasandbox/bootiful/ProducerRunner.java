package evil.inc.kafkasandbox.bootiful;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
public class ProducerRunner implements CommandLineRunner {

    private final ReactiveKafkaProducerTemplate<String, OrderEvent> template;

    public ProducerRunner(ReactiveKafkaProducerTemplate<String, OrderEvent> template) {
        this.template = template;
    }

    @Override
    public void run(String... args) throws Exception {
        this.orderEventFlux()
                .flatMap(o -> this.template.send("order-events-1", o.orderId().toString(), o))
                .doOnNext(r -> log.info("CorrelationMetadata : {} ", r.recordMetadata()))
                .subscribe();
    }

    private Flux<OrderEvent> orderEventFlux() {
        return Flux.interval(Duration.ofMillis(500))
                .take(1000)
                .map(i -> new OrderEvent(UUID.randomUUID(), i, LocalDateTime.now()));
    }
}
