package io.coffeebeans.connector.sink.format;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;

public interface RecordWriterProvider {

    RecordWriter getRecordWriter(AzureBlobSinkConfig config, String fileName);
}
