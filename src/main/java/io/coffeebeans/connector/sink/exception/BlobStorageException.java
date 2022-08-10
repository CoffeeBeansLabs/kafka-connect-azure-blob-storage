package io.coffeebeans.connector.sink.exception;

import org.apache.kafka.connect.errors.ConnectException;

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
