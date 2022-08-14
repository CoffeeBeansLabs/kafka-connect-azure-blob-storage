package io.coffeebeans.connector.sink.exception;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * Custom Exception for schema related errors.
 */
public class SchemaParseException extends ConnectException {
    private static final long serialVersionUID = 1L;

    public SchemaParseException(String s) {
        super(s);
    }

    public SchemaParseException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public SchemaParseException(Throwable throwable) {
        super(throwable);
    }
}
