package io.coffeebeans.connector.sink.format.bytearray;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.RecordWriterProvider;
import io.coffeebeans.connector.sink.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ByteArrayRecordWriterProvider} is used to get instance of
 * {@link ByteArrayRecordWriter}.
 */
public class ByteArrayRecordWriterProvider implements RecordWriterProvider {
    private final Logger log = LoggerFactory.getLogger(ByteArrayRecordWriterProvider.class);

    private int partSize;
    private String extension;
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

        this.partSize = config.getPartSize();
        this.extension = config.getBinaryFileExtension();
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

        String blobNameWithExtension = blobName + extension;

        return new ByteArrayRecordWriter(storageManager, partSize, blobNameWithExtension, kafkaTopic);
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
