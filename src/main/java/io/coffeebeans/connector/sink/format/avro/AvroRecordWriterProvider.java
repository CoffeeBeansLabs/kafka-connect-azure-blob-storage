package io.coffeebeans.connector.sink.format.avro;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.RecordWriterProvider;
import io.coffeebeans.connector.sink.format.SchemaStore;
import io.coffeebeans.connector.sink.storage.StorageManager;
import org.apache.avro.file.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroRecordWriterProvider implements RecordWriterProvider {
    private final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
    private static final String EXTENSION = ".avro";

    private CodecFactory codecFactory;
    private final SchemaStore schemaStore;

    public AvroRecordWriterProvider(SchemaStore schemaStore) {
        this.schemaStore = schemaStore;
    }

    @Override
    public RecordWriter getRecordWriter(AzureBlobSinkConfig config,
                                        StorageManager storageManager,
                                        String fileName,
                                        String topic) {

        if (this.codecFactory == null) {
            this.codecFactory = CodecFactory
                    .fromString(
                            config.getAvroCompressionCodec()
                    );
            log.info("Configured Avro compression codec: " + config.getAvroCompressionCodec());
        }

        String blobName = fileName + getExtension();
        int partSize = config.getPartSize();

        return new AvroRecordWriter(
                storageManager,
                schemaStore,
                partSize,
                blobName,
                topic,
                codecFactory
        );
    }

    public String getExtension() {
        return EXTENSION;
    }
}
