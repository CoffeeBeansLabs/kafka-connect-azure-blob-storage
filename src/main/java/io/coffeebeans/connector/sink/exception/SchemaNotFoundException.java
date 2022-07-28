package io.coffeebeans.connector.sink.exception;

public class SchemaNotFoundException extends Exception {
    private static final long serialVersionUID = 1L;

    public SchemaNotFoundException(String message) {
        super(message);
    }
}
