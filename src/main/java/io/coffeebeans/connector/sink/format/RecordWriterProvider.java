package io.coffeebeans.connector.sink.format;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;

/**
 * RecordWriterProvider is responsible for initializing and
 * providing new instance of suitable RecordWriter.
 */
public interface RecordWriterProvider {

    RecordWriter getRecordWriter(AzureBlobSinkConfig config, String fileName);
}
