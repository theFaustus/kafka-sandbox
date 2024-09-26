package evil.inc.kafkasandbox.bootiful;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ConsumerRunner implements CommandLineRunner {

    private final ReactiveKafkaConsumerTemplate<String, OrderEvent> template;

    public ConsumerRunner(ReactiveKafkaConsumerTemplate<String, OrderEvent> template) {
        this.template = template;
    }

    @Override
    public void run(String... args) throws Exception {

        this.template.receive()
                .doOnNext(r -> log.info("Received record key: {} and value: {} ", r.key(), r.value()))
                .subscribe();

    }
}
