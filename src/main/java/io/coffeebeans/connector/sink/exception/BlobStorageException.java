package io.coffeebeans.connector.sink.exception;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * Custom exception for wrapping blob storage exceptions.
 */
public class BlobStorageException extends ConnectException {

    public BlobStorageException(String s) {
        super(s);
    }

    public BlobStorageException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public BlobStorageException(Throwable throwable) {
        super(throwable);
    }
}
