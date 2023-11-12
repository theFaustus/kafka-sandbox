package evil.inc.kafkasandbox.kafkastreams.joins;

import java.time.LocalDateTime;

public record BankTransaction(String name, Double amount, LocalDateTime time) {
}
