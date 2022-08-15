package io.coffeebeans.connect.azure.blob.sink.format;

import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;

/**
 * RecordWriterProvider is responsible for initializing and
 * providing new instance of suitable RecordWriter.
 */
public interface RecordWriterProvider {

    void configure(AzureBlobSinkConfig config);

    RecordWriter getRecordWriter(String blobName, String kafkaTopic);
}
