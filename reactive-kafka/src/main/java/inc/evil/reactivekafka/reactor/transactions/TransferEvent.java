package inc.evil.reactivekafka.reactor.transactions;

public record TransferEvent(String key, String from, String to, String amount, Runnable ack) {}
