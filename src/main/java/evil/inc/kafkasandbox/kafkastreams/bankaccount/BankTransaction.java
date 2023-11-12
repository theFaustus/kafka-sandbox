package evil.inc.kafkasandbox.kafkastreams.bankaccount;

import java.time.LocalDateTime;

public record BankTransaction(String name, Double amount, LocalDateTime time) {
}
