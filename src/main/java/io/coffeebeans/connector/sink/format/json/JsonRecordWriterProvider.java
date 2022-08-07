package io.coffeebeans.connector.sink.format.json;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.RecordWriterProvider;
import io.coffeebeans.connector.sink.format.SchemaStore;
import io.coffeebeans.connector.sink.storage.StorageManager;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonRecordWriterProvider implements RecordWriterProvider {
    private final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
    private final String EXTENSION = ".json";

    private final SchemaStore schemaStore;

    public JsonRecordWriterProvider(SchemaStore schemaStore) {
        this.schemaStore = schemaStore;
    }

    @Override
    public RecordWriter getRecordWriter(AzureBlobSinkConfig config,
                                        StorageManager storageManager,
                                        String fileName,
                                        String kafkaTopic) {

        String blobName = fileName + EXTENSION;

        try {
            return new JsonRecordWriter(
                    storageManager,
                    schemaStore,
                    config.getPartSize(),
                    blobName,
                    kafkaTopic
            );

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
