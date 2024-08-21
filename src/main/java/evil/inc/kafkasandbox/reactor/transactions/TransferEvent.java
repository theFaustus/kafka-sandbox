package evil.inc.kafkasandbox.reactor.transactions;

public record TransferEvent(String key, String from, String to, String amount, Runnable ack) {}
