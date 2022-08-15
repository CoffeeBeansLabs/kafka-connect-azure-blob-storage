package io.coffeebeans.connect.azure.blob.sink.exception;

import org.apache.kafka.connect.errors.RetriableException;

/**
 * Custom exception to be thrown when an exception occurs during performing partition on incoming records.
 */
public class PartitionException extends RetriableException {

    public PartitionException(String s) {
        super(s);
    }

    public PartitionException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public PartitionException(Throwable throwable) {
        super(throwable);
    }
}
