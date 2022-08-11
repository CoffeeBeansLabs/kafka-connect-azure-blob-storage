package io.coffeebeans.connector.sink.format.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.format.AzureBlobOutputStream;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.SchemaStore;
import io.coffeebeans.connector.sink.storage.StorageManager;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

/**
 * To write data in avro file format from struct, json string or map.
 */
public class AvroRecordWriter implements RecordWriter {
    private final Logger log = LoggerFactory.getLogger(AvroRecordWriter.class);
    private static final int AVRO_DATA_CACHE_SIZE = 20;

    private Schema kafkaValueSchema;
    private AzureBlobOutputStream outputStream;
    private final DataFileWriter<Object> dataFileWriter;

    private final int partSize;
    private final String blobName;
    private final AvroData avroData;
    private final String kafkaTopic;
    private final ObjectMapper mapper;
    private final SchemaStore schemaStore;
    private final StorageManager storageManager;
    private org.apache.avro.Schema avroValueSchema;
    private final JsonAvroConverter jsonAvroConverter;

    /**
     * Constructs a {@link AvroRecordWriter}.
     *
     * @param storageManager Storage manager to interact with Azure blob storage
     * @param schemaStore Schema store to retrieve schemas for json string or json without schema
     * @param partSize Part size or buffer size
     * @param blobName Name of the blob
     * @param kafkaTopic Kafka topic name
     */
    public AvroRecordWriter(StorageManager storageManager,
                            SchemaStore schemaStore,
                            int partSize,
                            String blobName,
                            String kafkaTopic) {

        this.partSize = partSize;
        this.blobName = blobName;
        this.kafkaTopic = kafkaTopic;
        this.schemaStore = schemaStore;
        this.mapper = new ObjectMapper();
        this.storageManager = storageManager;
        this.avroData = new AvroData(AVRO_DATA_CACHE_SIZE);
        this.jsonAvroConverter = new JsonAvroConverter();

        this.dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>());
    }

    @Override
    public void write(SinkRecord kafkaRecord) throws IOException {
        if (kafkaRecord.value() instanceof String) {
            /*
            For Json string values
             */
            write((String) kafkaRecord.value());
            return;

        } else if (kafkaRecord.value() instanceof Map) {
            /*
            For Json without embedded schema or schema registry
             */
            String jsonString = mapper
                    .writeValueAsString(
                            kafkaRecord.value()
                    );
            write(jsonString);
            return;
        }

        if (kafkaValueSchema == null) {
            log.info("Opening Avro record writer for blob: {}", blobName);

            kafkaValueSchema = kafkaRecord.valueSchema();
            avroValueSchema = avroData
                    .fromConnectSchema(kafkaValueSchema);

            outputStream = new AzureBlobOutputStream(
                    storageManager,
                    blobName,
                    partSize
            );
            dataFileWriter.create(avroValueSchema, outputStream);
        }
        Object value = avroData
                .fromConnectData(kafkaValueSchema, kafkaRecord.value());

        if (value instanceof NonRecordContainer) {
            value = ((NonRecordContainer) value).getValue();
        }
        dataFileWriter.append(value);
    }

    private void write(String value) throws IOException {
        if (avroValueSchema == null) {
            log.info("Opening Avro record writer for blob: {}", blobName);

            avroValueSchema = (org.apache.avro.Schema) schemaStore
                    .getSchema(kafkaTopic);

            outputStream = new AzureBlobOutputStream(
                    storageManager,
                    blobName,
                    partSize
            );
            dataFileWriter.create(avroValueSchema, outputStream);
        }
        Object record = jsonAvroConverter
                .convertToGenericDataRecord(value.getBytes(StandardCharsets.UTF_8), avroValueSchema);

        dataFileWriter.append(record);
    }

    @Override
    public void close() throws IOException {
        dataFileWriter.close();
    }

    @Override
    public void commit(boolean ensureCommitted) throws IOException {
        dataFileWriter.flush();
        outputStream.commit(ensureCommitted);
        dataFileWriter.close();
    }

    @Override
    public long getDataSize() {
        return 0L;
    }
}
