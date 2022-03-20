package io.coffeebeans.connector.sink.exception;

import org.apache.kafka.common.KafkaException;

/**
 * Custom exception to be thrown when an exception occurs during performing partition on incoming records.
 */
public class PartitionException extends KafkaException {
    private static final long serialVersionUID = 1L;

    public PartitionException(String message) {
        super(message);
    }
}
