package evil.inc.kafkasandbox.kafkastreams.joins;

import java.time.LocalDateTime;

public record BankBalance(int count, double balance, LocalDateTime time) {
}
