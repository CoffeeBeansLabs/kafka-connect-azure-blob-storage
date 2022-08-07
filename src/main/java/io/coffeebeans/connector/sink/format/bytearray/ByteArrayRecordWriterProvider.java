package io.coffeebeans.connector.sink.format.bytearray;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.RecordWriterProvider;
import io.coffeebeans.connector.sink.format.SchemaStore;
import io.coffeebeans.connector.sink.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteArrayRecordWriterProvider implements RecordWriterProvider {
    private final Logger log = LoggerFactory.getLogger(ByteArrayRecordWriterProvider.class);
    private final String EXTENSION = ".bin";

    private final SchemaStore schemaStore;

    public ByteArrayRecordWriterProvider(SchemaStore schemaStore) {
        this.schemaStore = schemaStore;
    }

    @Override
    public RecordWriter getRecordWriter(AzureBlobSinkConfig config,
                                        StorageManager storageManager,
                                        String fileName,
                                        String kafkaTopic) {

        String blobName = fileName + EXTENSION;

        return new ByteArrayRecordWriter(
                storageManager,
                schemaStore,
                config.getPartSize(),
                blobName,
                kafkaTopic
        );
    }
}
