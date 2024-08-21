package evil.inc.kafkasandbox.reactor.error.deadletter;

import reactor.kafka.receiver.ReceiverRecord;

public class RecordProcessingException extends RuntimeException {

    private final ReceiverRecord<?, ?> receiverRecord;

    public RecordProcessingException(ReceiverRecord<?, ?> receiverRecord, Throwable e) {
        super(e);
        this.receiverRecord = receiverRecord;
    }

    @SuppressWarnings("unchecked")
    public <K, V> ReceiverRecord<K, V> getReceiverRecord() {
        return (ReceiverRecord<K, V>) receiverRecord;
    }
}
