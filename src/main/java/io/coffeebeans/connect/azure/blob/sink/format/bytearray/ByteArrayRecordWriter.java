package io.coffeebeans.connect.azure.blob.sink.format.bytearray;

import io.coffeebeans.connect.azure.blob.sink.format.AzureBlobOutputStream;
import io.coffeebeans.connect.azure.blob.sink.format.CompressionType;
import io.coffeebeans.connect.azure.blob.sink.format.RecordWriter;
import io.coffeebeans.connect.azure.blob.sink.storage.StorageManager;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes data from {@link SinkRecord#value()} to blob storage in byte array format.
 */
public class ByteArrayRecordWriter implements RecordWriter {
    private static final Logger log = LoggerFactory.getLogger(ByteArrayRecordWriter.class);
    private static final byte[] LINE_SEPARATOR_BYTES = System.lineSeparator()
            .getBytes(StandardCharsets.UTF_8);

    private final String kafkaTopic;
    private final AzureBlobOutputStream outputStream;
    private final ByteArrayConverter byteArrayConverter;
    private final OutputStream outputStreamCompressionWrapper;

    /**
     * Constructs {@link ByteArrayRecordWriter}.
     *
     * @param storageManager Storage manager to interact with Azure blob storage
     * @param compressionType Compression type
     * @param compressionLevel Level of compression
     * @param blockSize Block size
     * @param blobName Blob name
     * @param kafkaTopic Kafka topic
     */
    public ByteArrayRecordWriter(StorageManager storageManager,
                                 CompressionType compressionType,
                                 int compressionLevel,
                                 int blockSize,
                                 String blobName,
                                 String kafkaTopic) {

        this.kafkaTopic = kafkaTopic;

        this.outputStream = new AzureBlobOutputStream(storageManager, blobName, blockSize)
                .setCompressionLevel(compressionLevel)
                .setCompressionType(compressionType);

        this.outputStreamCompressionWrapper = this.outputStream
                .wrapForCompression();

        Map<String, String> configProp = new HashMap<>();
        this.byteArrayConverter = new ByteArrayConverter();
        this.byteArrayConverter.configure(configProp, false);

        log.debug("Opened ByteArray record writer for blob name: {}", blobName);
    }

    @Override
    public void write(SinkRecord kafkaRecord) {
        try {
            byte[] bytes = byteArrayConverter.fromConnectData(
                    kafkaTopic,
                    kafkaRecord.valueSchema(),
                    kafkaRecord.value()
            );
            this.outputStreamCompressionWrapper.write(bytes);
            this.outputStreamCompressionWrapper.write(LINE_SEPARATOR_BYTES);

        } catch (Exception e) {
            throw new RetriableException(e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void commit() {
        try {
            this.outputStream.commit();
            this.outputStreamCompressionWrapper.close();

        } catch (Exception e) {
            throw new RetriableException(e);
        }
    }
}
