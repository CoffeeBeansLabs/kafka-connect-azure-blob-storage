package io.coffeebeans.connector.sink.format.parquet;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.storage.RecordWriter;
import io.coffeebeans.connector.sink.storage.RecordWriterProvider;
import io.confluent.connect.avro.AvroData;

public class ParquetRecordWriterProvider implements RecordWriterProvider {
    private static final String EXTENSION = ".parquet";

    private final AvroData avroData;

    public ParquetRecordWriterProvider(AvroData avroData) {
        this.avroData = avroData;
    }

    public String getExtension() {
        return EXTENSION;
    }

    public RecordWriter getRecordWriter(final AzureBlobSinkConfig config, final String fileName) {
        String blobName = fileName + getExtension();
        return new ParquetRecordWriter(config, avroData, blobName);
    }
}