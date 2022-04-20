package io.coffeebeans.connector.sink.format.parquet;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.storage.RecordWriter;
import io.confluent.connect.avro.AvroData;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ParquetRecordWriter implements RecordWriter {
    private static final Logger logger = LoggerFactory.getLogger(ParquetRecordWriter.class);
    private static final int PAGE_SIZE = 64 * 1024;

    private Schema kafkaSchema;
    private final String blobName;
    private final AvroData avroData;
    private final ObjectMapper objectMapper;
    private ParquetOutputFile outputFile;
    private final AzureBlobSinkConfig config;
    private org.apache.avro.Schema avroSchema;
    private ParquetWriter<GenericRecord> writer;

    ParquetRecordWriter(AzureBlobSinkConfig config, AvroData avroData, String blobName) {
        this.config = config;
        this.avroData = avroData;
        this.blobName = blobName;
        this.kafkaSchema = null;
        this.avroSchema = null;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void write(SinkRecord sinkRecord) throws IOException {
        if (sinkRecord.value() instanceof String) {
            writeJsonString(sinkRecord.value());
            return;
        }

        if (kafkaSchema == null || writer == null) {
            logger.info("Opening parquet record writer for blob: {}", blobName);

            kafkaSchema = sinkRecord.valueSchema();
            org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(kafkaSchema);

            outputFile = new ParquetOutputFile(config, blobName);
            AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter.<GenericRecord>builder(outputFile)
                    .withSchema(avroSchema)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withDictionaryEncoding(true)
                    .withPageSize(PAGE_SIZE);

            if (schemaHasArrayOfOptionalItems(kafkaSchema, /*seenSchemas=*/null)) {
                // If the schema contains an array of optional items, then
                // it is possible that the array may have null items during the
                // writing process.  In this case, we set a flag so as not to
                // incur a NullPointerException
                logger.debug(
                        "Setting \"" + AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE
                                + "\" to false because the schema contains an array "
                                + "with optional items"
                );
                builder.config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false");
            }
            writer = builder.build();
        }

        Object value = avroData.fromConnectData(kafkaSchema, sinkRecord.value());
        writer.write((GenericRecord) value);
    }

    private void writeJsonString(Object value) throws IOException {
        if (this.avroSchema == null || writer == null) {
            logger.info("Opening parquet record writer for blob: {}", blobName);

            this.avroSchema = JsonStringSchema.getSchema();
            if (this.avroSchema == null) {
                logger.info("Loading schema ..................................");
                JsonStringSchema.avroSchema = new org.apache.avro.Schema.Parser()
                        .parse(config.getAvroSchema());
                this.avroSchema = JsonStringSchema.getSchema();
            }

            outputFile = new ParquetOutputFile(config, blobName);
            AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter.<GenericRecord>builder(outputFile)
                    .withSchema(avroSchema)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withDictionaryEncoding(true)
                    .withPageSize(PAGE_SIZE);

            writer = builder.build();
        }
        Object obj = parseJsonString((String) value);
        writer.write((GenericRecord) obj);
    }

    private Object parseJsonString(String jsonString) throws IOException {
        try {
            GenericRecord record = new GenericData.Record(this.avroSchema);
            Map<String, Object> valueMap = objectMapper.readValue(jsonString,
                    new TypeReference<HashMap<String, Object>>() {});

            valueMap.forEach(record::put);
            return record;

        } catch (Exception e) {
            logger.error("Failed to process record {}, with exception: {},  skipping record .......", jsonString,
                    e.getMessage());
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        logger.info("ParquetRecordWriter: close initiated");
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public void commit() throws IOException {
        logger.info("ParquetRecordWriter: commit initiated");
        outputFile.getOutputStream().setCommit(true);
        if (writer != null) {
            writer.close();
        }
    }

    public static boolean schemaHasArrayOfOptionalItems(Schema schema, Set<Schema> seenSchemas) {
        // First, check for infinitely recursing schemas
        if (seenSchemas == null) {
            seenSchemas = new HashSet<>();
        } else if (seenSchemas.contains(schema)) {
            return false;
        }
        seenSchemas.add(schema);
        switch (schema.type()) {
            case STRUCT:
                for (Field field : schema.fields()) {
                    if (schemaHasArrayOfOptionalItems(field.schema(), seenSchemas)) {
                        return true;
                    }
                }
                return false;
            case MAP:
                return schemaHasArrayOfOptionalItems(schema.valueSchema(), seenSchemas);
            case ARRAY:
                return schema.valueSchema().isOptional()
                        || schemaHasArrayOfOptionalItems(schema.valueSchema(), seenSchemas);
            default:
                return false;
        }
    }

    @Override
    public long getDataSize() {
        if (writer == null) {
            return 0L;
        }
        return this.writer.getDataSize();
    }
}
