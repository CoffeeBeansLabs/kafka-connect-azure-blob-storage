package io.coffeebeans.connector.sink.exception;

/**
 * Custom Exception to be thrown when schema configuration not found.
 */
public class SchemaNotFoundException extends Exception {
    private static final long serialVersionUID = 1L;

    public SchemaNotFoundException(String message) {
        super(message);
    }
}
