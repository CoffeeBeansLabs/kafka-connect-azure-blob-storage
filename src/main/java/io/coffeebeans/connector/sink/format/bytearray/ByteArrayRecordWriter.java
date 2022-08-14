package io.coffeebeans.connector.sink.format.bytearray;

import io.coffeebeans.connector.sink.format.AzureBlobOutputStream;
import io.coffeebeans.connector.sink.format.CompressionType;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.storage.StorageManager;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteArrayRecordWriter implements RecordWriter {
    private final Logger log = LoggerFactory.getLogger(ByteArrayRecordWriter.class);
    private final byte[] LINE_SEPARATOR_BYTES = System.lineSeparator()
            .getBytes(StandardCharsets.UTF_8);

    private final String kafkaTopic;
    private final AzureBlobOutputStream outputStream;
    private final ByteArrayConverter byteArrayConverter;
    private final OutputStream outputStreamCompressionWrapper;

    public ByteArrayRecordWriter(StorageManager storageManager,
                                 CompressionType compressionType,
                                 int compressionLevel,
                                 int partSize,
                                 String blobName,
                                 String kafkaTopic) {

        this.kafkaTopic = kafkaTopic;

        this.outputStream = new AzureBlobOutputStream(storageManager, blobName, partSize)
                .setCompressionLevel(compressionLevel)
                .setCompressionType(compressionType);

        this.outputStreamCompressionWrapper = this.outputStream
                .wrapForCompression();

        Map<String, String> configProp = new HashMap<>();
        this.byteArrayConverter = new ByteArrayConverter();
        this.byteArrayConverter.configure(configProp, false);
    }

    @Override
    public void write(SinkRecord kafkaRecord) throws IOException {
        byte[] bytes = byteArrayConverter.fromConnectData(
                kafkaTopic,
                kafkaRecord.valueSchema(),
                kafkaRecord.value()
        );
        this.outputStreamCompressionWrapper.write(bytes);
        this.outputStreamCompressionWrapper.write(LINE_SEPARATOR_BYTES);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void commit() throws IOException {
        this.outputStream.commit();
        this.outputStreamCompressionWrapper.close();
    }

    @Override
    public long getDataSize() {
        return 0;
    }
}
