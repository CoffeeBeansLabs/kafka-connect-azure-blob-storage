package io.coffeebeans.connector.sink.exception;

import org.apache.kafka.common.KafkaException;

/**
 * Custom Exception.
 */
public class UnsupportedException extends KafkaException {
    private static final long serialVersionUID = 1L;

    public UnsupportedException(String message) {
        super(message);
    }
}
