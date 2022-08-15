package io.coffeebeans.connect.azure.blob.sink.format.bytearray;

import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connect.azure.blob.sink.format.CompressionType;
import io.coffeebeans.connect.azure.blob.sink.format.RecordWriter;
import io.coffeebeans.connect.azure.blob.sink.format.RecordWriterProvider;
import io.coffeebeans.connect.azure.blob.sink.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ByteArrayRecordWriterProvider} is used to get instance of
 * {@link ByteArrayRecordWriter}.
 */
public class ByteArrayRecordWriterProvider implements RecordWriterProvider {
    private static final Logger log = LoggerFactory.getLogger(ByteArrayRecordWriterProvider.class);

    private int blockSize;
    private String extension;
    private int compressionLevel;
    private CompressionType compressionType;
    private final StorageManager storageManager;

    /**
     * Constructs {@link ByteArrayRecordWriterProvider}.
     *
     * @param storageManager Storage manager to interact with Azure blob storage
     */
    public ByteArrayRecordWriterProvider(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    /**
     * Configures {@link ByteArrayRecordWriterProvider} based on the<br>
     * connector configuration.
     *
     * @param config Connector configuration
     */
    @Override
    public void configure(AzureBlobSinkConfig config) {

        this.blockSize = config.getBlockSize();
        this.extension = config.getBinaryFileExtension();
        this.compressionLevel = config.getCompressionLevel();

        configureCompressionType(
                config.getCompressionType()
        );
    }

    /**
     * Instantiates and return an instance of {@link ByteArrayRecordWriter}.
     *
     * @param blobName Blob name
     * @param kafkaTopic Kafka topic
     * @return Instance of ByteArrayRecordWriter
     */
    @Override
    public RecordWriter getRecordWriter(String blobName,
                                        String kafkaTopic) {

        String blobNameWithExtension = blobName + extension + compressionType.extension;

        return new ByteArrayRecordWriter(
                storageManager,
                compressionType,
                compressionLevel,
                blockSize, blobNameWithExtension, kafkaTopic);
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
     * Extension of binary files.
     *
     * @return extension
     */
    public String getExtension() {
        return extension;
    }
}
