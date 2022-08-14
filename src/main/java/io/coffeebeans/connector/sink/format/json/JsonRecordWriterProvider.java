package io.coffeebeans.connector.sink.format.json;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.CompressionType;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.RecordWriterProvider;
import io.coffeebeans.connector.sink.storage.StorageManager;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link JsonRecordWriterProvider} is used to get instance of
 * {@link JsonRecordWriter}.
 */
public class JsonRecordWriterProvider implements RecordWriterProvider {
    private final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
    private final String EXTENSION = ".json";

    private int partSize;
    private int schemasCacheSize;
    private int compressionLevel;
    private CompressionType compressionType;
    private final StorageManager storageManager;

    /**
     * Constructs {@link JsonRecordWriterProvider}.
     *
     * @param storageManager Storage manager to interact with Azure blob storage
     */
    public JsonRecordWriterProvider(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    /**
     * Configures {@link JsonRecordWriterProvider} based on the<br>
     * connector configuration.
     *
     * @param config Connector configuration
     */
    @Override
    public void configure(AzureBlobSinkConfig config) {

        this.partSize = config.getPartSize();
        this.schemasCacheSize = config.getSchemaCacheSize();
        this.compressionLevel = config.getCompressionLevel();

        configureCompressionType(
                config.getCompressionType()
        );
    }

    /**
     * Instantiates and returns a new instance of {@link JsonRecordWriter}.
     *
     * @param blobName Blob name
     * @param kafkaTopic Kafka topic
     * @return Instance of Record writer
     */
    @Override
    public RecordWriter getRecordWriter(String blobName, String kafkaTopic) {

        String blobNameWithExtension = blobName + EXTENSION + compressionType.extension;

        try {
            return new JsonRecordWriter(
                    storageManager,
                    compressionType,
                    compressionLevel,
                    partSize, blobNameWithExtension, schemasCacheSize);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void configureCompressionType(String compressionType) {
        this.compressionType = CompressionType
                .forName(compressionType);
    }

    /**
     * Extension of the JSON files.
     *
     * @return Extension
     */
    public String getExtension() {
        return EXTENSION;
    }
}
