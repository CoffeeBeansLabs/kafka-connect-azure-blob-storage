package io.coffeebeans.connector.sink.exception;

import org.apache.kafka.common.KafkaException;

public class PartitionException extends KafkaException {
    private static final long serialVersionUID = 1L;

    public PartitionException(String message) {
        super(message);
    }
}
