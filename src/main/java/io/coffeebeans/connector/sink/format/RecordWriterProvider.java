package io.coffeebeans.connector.sink.format;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.storage.StorageManager;

/**
 * RecordWriterProvider is responsible for initializing and
 * providing new instance of suitable RecordWriter.
 */
public interface RecordWriterProvider {

    RecordWriter getRecordWriter(AzureBlobSinkConfig config,
                                 StorageManager storageManager,
                                 String fileName,
                                 String topic);
}
