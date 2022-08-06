package io.coffeebeans.connector.sink.format.avro;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.RecordWriterProvider;
import io.coffeebeans.connector.sink.format.SchemaStore;
import io.coffeebeans.connector.sink.storage.StorageManager;

public class AvroRecordWriterProvider implements RecordWriterProvider {
    private static final String EXTENSION = ".avro";

    private final SchemaStore schemaStore;

    public AvroRecordWriterProvider(SchemaStore schemaStore) {
        this.schemaStore = schemaStore;
    }

    @Override
    public RecordWriter getRecordWriter(AzureBlobSinkConfig config,
                                        StorageManager storageManager,
                                        String fileName,
                                        String topic) {

        String blobName = fileName + getExtension();
        int partSize = config.getPartSize();

        return new AvroRecordWriter(
                storageManager,
                schemaStore,
                partSize,
                blobName,
                topic
        );
    }

    public String getExtension() {
        return EXTENSION;
    }
}
