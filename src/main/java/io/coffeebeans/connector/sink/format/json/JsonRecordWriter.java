package io.coffeebeans.connector.sink.format.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.format.AzureBlobOutputStream;
import io.coffeebeans.connector.sink.format.CompressionType;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.storage.StorageManager;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Write data from {@link SinkRecord#value()} to blob storage in JSON format.
 */
public class JsonRecordWriter implements RecordWriter {
    private static final Logger log = LoggerFactory.getLogger(JsonRecordWriter.class);
    private static final String LINE_SEPARATOR = System.lineSeparator();
    private static final byte[] LINE_SEPARATOR_BYTES = System.lineSeparator()
            .getBytes(StandardCharsets.UTF_8);

    private final JsonConverter jsonConverter;
    private final JsonGenerator jsonGenerator;
    private final AzureBlobOutputStream outputStream;
    private final OutputStream outputStreamCompressionWrapper;

    /**
     * Constructs {@link JsonRecordWriter}.
     *
     * @param storageManager Storage manager to interact with Azure blob storage.
     * @param compressionType Compression type
     * @param compressionLevel Level of compression
     * @param blockSize Block size
     * @param blobName Blob name
     * @param schemasCacheSize Schema cache size
     * @throws IOException Throws if encounters any error while opening record
     *      writer or while writing the record.
     */
    public JsonRecordWriter(StorageManager storageManager,
                            CompressionType compressionType,
                            int compressionLevel,
                            int blockSize,
                            String blobName,
                            int schemasCacheSize) throws IOException {

        this.jsonConverter = new JsonConverter();

        this.outputStream = new AzureBlobOutputStream(storageManager, blobName, blockSize)
                .setCompressionLevel(compressionLevel)
                .setCompressionType(compressionType);

        this.outputStreamCompressionWrapper = this.outputStream
                .wrapForCompression();

        this.jsonGenerator = new ObjectMapper()
                .getFactory()
                .createGenerator(outputStreamCompressionWrapper)
                .setRootValueSeparator(null);

        Map<String, String> converterConfig = new HashMap<>() {
            {
                put("schemas.enable", "false");
                put("schemas.cache.size", String.valueOf(schemasCacheSize));
            }
        };
        this.jsonConverter.configure(converterConfig, false);
        log.debug("Opened JSON record writer for blob name: {}", blobName);
    }

    @Override
    public void write(SinkRecord kafkaRecord) throws IOException {
        Object value = kafkaRecord.value();

        if (value instanceof Struct) {
            byte[] rawJson = jsonConverter.fromConnectData(
                    kafkaRecord.topic(),
                    kafkaRecord.valueSchema(),
                    value
            );
            outputStreamCompressionWrapper.write(rawJson);
            outputStreamCompressionWrapper.write(LINE_SEPARATOR_BYTES);
            return;
        }

        jsonGenerator.writeObject(value);
        jsonGenerator.writeRaw(LINE_SEPARATOR);
    }

    @Override
    public void close() throws IOException {
        jsonGenerator.close();
    }

    @Override
    public void commit() throws IOException {
        jsonGenerator.flush();
        outputStream.commit();
        outputStreamCompressionWrapper.close();
    }

    @Override
    public long getDataSize() {
        return 0;
    }
}
