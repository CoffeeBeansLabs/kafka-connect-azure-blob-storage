package io.coffeebeans.connector.sink.exception;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * Custom Exception.
 */
public class UnsupportedOperationException extends ConnectException {
    private static final long serialVersionUID = 1L;

    public UnsupportedOperationException(String s) {
        super(s);
    }

    public UnsupportedOperationException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public UnsupportedOperationException(Throwable throwable) {
        super(throwable);
    }
}
