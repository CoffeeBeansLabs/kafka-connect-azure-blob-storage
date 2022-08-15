package io.coffeebeans.connect.azure.blob.sink.format.json;

import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connect.azure.blob.sink.format.CompressionType;
import io.coffeebeans.connect.azure.blob.sink.format.RecordWriter;
import io.coffeebeans.connect.azure.blob.sink.format.RecordWriterProvider;
import io.coffeebeans.connect.azure.blob.sink.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link JsonRecordWriterProvider} is used to get instance of
 * {@link JsonRecordWriter}.
 */
public class JsonRecordWriterProvider implements RecordWriterProvider {
    private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
    private static final String EXTENSION = ".json";

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

        this.partSize = config.getBlockSize();
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

        return new JsonRecordWriter(
                storageManager,
                compressionType,
                compressionLevel,
                partSize, blobNameWithExtension, schemasCacheSize);
    }

    /**
     * Configures the {@link CompressionType}.<br>
     * It configures it based on the {@link AzureBlobSinkConfig#COMPRESSION_TYPE_CONF az.compression.type}<br>
     * property configured by the user.
     * <br>
     *
     * @param compressionType Connector configuration
     */
    private void configureCompressionType(String compressionType) {
        this.compressionType = CompressionType
                .forName(compressionType);

        log.debug("Configured compression of type: {}", compressionType);
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
