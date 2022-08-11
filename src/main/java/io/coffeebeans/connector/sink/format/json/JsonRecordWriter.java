package io.coffeebeans.connector.sink.format.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.format.AzureBlobOutputStream;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.SchemaStore;
import io.coffeebeans.connector.sink.storage.StorageManager;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonRecordWriter implements RecordWriter {
    private final Logger log = LoggerFactory.getLogger(JsonRecordWriter.class);
    private final String LINE_SEPARATOR = System.lineSeparator();
    private final byte[] LINE_SEPARATOR_BYTES = System.lineSeparator()
            .getBytes(StandardCharsets.UTF_8);

    private final int partSize;
    private final String blobName;
    private final String kafkaTopic;
    private final ObjectMapper mapper;
    private final SchemaStore schemaStore;
    private final JsonConverter jsonConverter;
    private final JsonGenerator jsonGenerator;
    private final StorageManager storageManager;
    private final AzureBlobOutputStream outputStream;

    public JsonRecordWriter(StorageManager storageManager,
                            SchemaStore schemaStore,
                            int partSize,
                            String blobName,
                            String kafkaTopic) throws IOException {

        this.partSize = partSize;
        this.blobName = blobName;
        this.kafkaTopic = kafkaTopic;
        this.schemaStore = schemaStore;
        this.storageManager = storageManager;

        this.mapper = new ObjectMapper();
        this.jsonConverter = new JsonConverter();
        this.outputStream = new AzureBlobOutputStream(storageManager, blobName, partSize);

        this.jsonGenerator = mapper
                .getFactory()
                .createGenerator(outputStream)
                .setRootValueSeparator(null);

        Map<String, String> converterConfig = new HashMap<>() {
            {
                put("schemas.enable", "false");
                put("schemas.cache.size", "20");
            }
        };
        this.jsonConverter.configure(converterConfig, false);
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
            outputStream.write(rawJson);
            outputStream.write(LINE_SEPARATOR_BYTES);
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
    public void commit(boolean ensureCommitted) throws IOException {
        jsonGenerator.flush();
        outputStream.commit(ensureCommitted);
        outputStream.close();
    }

    @Override
    public long getDataSize() {
        return 0;
    }
}
