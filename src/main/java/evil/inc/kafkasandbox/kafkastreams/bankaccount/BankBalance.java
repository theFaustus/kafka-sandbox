package evil.inc.kafkasandbox.kafkastreams.bankaccount;

import java.time.LocalDateTime;

public record BankBalance(int count, double balance, LocalDateTime time) {
}
