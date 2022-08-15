package io.coffeebeans.connect.azure.blob.sink.format.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connect.azure.blob.sink.format.AzureBlobOutputStream;
import io.coffeebeans.connect.azure.blob.sink.format.RecordWriter;
import io.coffeebeans.connect.azure.blob.sink.format.SchemaStore;
import io.coffeebeans.connect.azure.blob.sink.storage.StorageManager;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

/**
 * Writes data from {@link SinkRecord#value()} to blob storage in Avro format.
 */
public class AvroRecordWriter implements RecordWriter {
    private static final Logger log = LoggerFactory.getLogger(AvroRecordWriter.class);

    private Schema kafkaValueSchema;
    private AzureBlobOutputStream outputStream;
    private final DataFileWriter<Object> dataFileWriter;

    private final int blockSize;
    private final String blobName;
    private final AvroData avroData;
    private final String kafkaTopic;
    private final ObjectMapper mapper;
    private final SchemaStore schemaStore;
    private final CodecFactory codecFactory;
    private final StorageManager storageManager;
    private org.apache.avro.Schema avroValueSchema;
    private final JsonAvroConverter jsonAvroConverter;

    /**
     * Constructs a {@link AvroRecordWriter}.
     *
     * @param storageManager Storage manager to interact with Azure blob storage
     * @param schemaStore Schema store to retrieve schemas for json string or json without schema
     * @param blockSize Part size or buffer size
     * @param blobName Name of the blob
     * @param kafkaTopic Kafka topic name
     */
    public AvroRecordWriter(StorageManager storageManager,
                            SchemaStore schemaStore,
                            int blockSize,
                            String blobName,
                            String kafkaTopic,
                            CodecFactory codecFactory,
                            AvroData avroData) {

        this.blockSize = blockSize;
        this.blobName = blobName;
        this.avroData = avroData;
        this.kafkaTopic = kafkaTopic;
        this.schemaStore = schemaStore;
        this.codecFactory = codecFactory;
        this.storageManager = storageManager;

        this.mapper = new ObjectMapper();
        this.jsonAvroConverter = new JsonAvroConverter();
        this.dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>());
    }

    @Override
    public void write(SinkRecord kafkaRecord) throws RetriableException {
        try {
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
                log.debug("Opening Avro record writer for blob: {}", blobName);

                kafkaValueSchema = kafkaRecord.valueSchema();
                avroValueSchema = avroData
                        .fromConnectSchema(kafkaValueSchema);

                outputStream = new AzureBlobOutputStream(
                        storageManager,
                        blobName,
                        blockSize
                );
                dataFileWriter.setCodec(codecFactory);
                dataFileWriter.create(avroValueSchema, outputStream);
            }
            Object value = avroData
                    .fromConnectData(kafkaValueSchema, kafkaRecord.value());

            if (value instanceof NonRecordContainer) {
                value = ((NonRecordContainer) value).getValue();
            }
            dataFileWriter.append(value);

        } catch (Exception e) {
            throw new RetriableException(e);
        }
    }

    private void write(String value) throws IOException {
        if (avroValueSchema == null) {
            log.debug("Opening Avro record writer for blob: {}", blobName);

            avroValueSchema = (org.apache.avro.Schema) schemaStore
                    .getSchema(kafkaTopic);

            outputStream = new AzureBlobOutputStream(
                    storageManager,
                    blobName,
                    blockSize
            );
            dataFileWriter.setCodec(codecFactory);
            dataFileWriter.create(avroValueSchema, outputStream);
        }
        Object record = jsonAvroConverter
                .convertToGenericDataRecord(value.getBytes(StandardCharsets.UTF_8), avroValueSchema);

        dataFileWriter.append(record);
    }

    @Override
    public void close() throws RetriableException {
        try {
            dataFileWriter.close();

        } catch (IOException e) {
            throw new RetriableException(e);
        }
    }

    @Override
    public void commit() throws RetriableException {
        try {
            dataFileWriter.flush();
            outputStream.commit();
            dataFileWriter.close();

        } catch (IOException e) {
            throw new RetriableException(e);
        }
    }
}
