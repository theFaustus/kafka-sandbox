package evil.inc.kafkasandbox.bootiful;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ReceiverOptions<String, OrderEvent> receiverOptions(KafkaProperties props) {
        return ReceiverOptions.<String, OrderEvent>create(props.buildConsumerProperties())
                .subscription(List.of("order-events-1"));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, OrderEvent> consumerTemplate(ReceiverOptions<String, OrderEvent> options) {
        return new ReactiveKafkaConsumerTemplate<>(options);
    }
}
