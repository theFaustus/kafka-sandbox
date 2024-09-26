package evil.inc.kafkasandbox.bootiful;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.SenderOptions;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public SenderOptions<String, OrderEvent> senderOptions(KafkaProperties props) {
        return SenderOptions.<String, OrderEvent>create(props.buildProducerProperties());
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, OrderEvent> producerTemplate(SenderOptions<String, OrderEvent> options) {
        return new ReactiveKafkaProducerTemplate<>(options);
    }
}
