package io.coffeebeans.connector.sink.format.parquet;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.SchemaStore;
import io.coffeebeans.connector.sink.format.avro.AvroSchemaStore;
import io.coffeebeans.connector.sink.storage.StorageManager;
import io.confluent.connect.avro.AvroData;
import java.io.IOException;
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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

/**
 *  Writes data from {@link SinkRecord#value()} to blob storage in Parquet file format.
 */
public class ParquetRecordWriter implements RecordWriter {
    private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriter.class);
    private static final int PAGE_SIZE = 64 * 1024;

    private final String topic;
    private final int blockSize;
    private Schema kafkaSchema;
    private ParquetWriter writer;
    private final String blobName;
    private final AvroData avroData;
    private final ObjectMapper mapper;
    private final SchemaStore schemaStore;
    private JsonAvroConverter converter;
    private ParquetOutputFile outputFile;
    private final StorageManager storageManager;
    private org.apache.avro.Schema avroSchema;
    private final CompressionCodecName compressionCodec;

    /**
     * Constructs {@link ParquetRecordWriter}.
     *
     * @param storageManager Storage manager to interact with Azure blob storage
     * @param schemaStore Schema store to retrieve avro schemas for JSON string / JSON without schema
     * @param blockSize Block size
     * @param blobName Blob name
     * @param kafkaTopic Kafka topic
     * @param codec Compression codec
     * @param avroData AvroData
     */
    public ParquetRecordWriter(StorageManager storageManager,
                               SchemaStore schemaStore,
                               int blockSize,
                               String blobName,
                               String kafkaTopic,
                               CompressionCodecName codec,
                               AvroData avroData) {

        this.kafkaSchema = null;
        this.avroSchema = null;

        this.topic = kafkaTopic;
        this.blockSize = blockSize;
        this.blobName = blobName;
        this.avroData = avroData;
        this.schemaStore = schemaStore;
        this.storageManager = storageManager;
        this.compressionCodec = codec;

        this.mapper = new ObjectMapper();
    }

    /**
     * It will extract the kafka schema from the <code>sinkRecord</code>
     * value and convert it to Avro schema. It initializes
     * {@link ParquetOutputFile} and {@link AvroParquetWriter} to write
     * Parquet files.
     *
     * <p>It also checks for if the schema has an array of optional items.
     * It converts the value object to {@link GenericRecord} and write
     * using the ParquetWriter.
     *
     * @param kafkaRecord sink record to be processed
     * @throws IOException if any I/O error occur
     */
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

        if (kafkaSchema == null || writer == null) {
            log.debug("Opening parquet record writer for blob: {}", blobName);

            kafkaSchema = kafkaRecord.valueSchema();
            org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(kafkaSchema);

            outputFile = new ParquetOutputFile(
                    this.storageManager,
                    this.blobName,
                    this.blockSize
            );
            AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter.<GenericRecord>builder(outputFile)
                    .withSchema(avroSchema)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withDictionaryEncoding(true)
                    .withCompressionCodec(compressionCodec)
                    .withPageSize(PAGE_SIZE);

            if (schemaHasArrayOfOptionalItems(kafkaSchema, /*seenSchemas=*/null)) {
                // If the schema contains an array of optional items, then
                // it is possible that the array may have null items during the
                // writing process.  In this case, we set a flag so as not to
                // incur a NullPointerException
                log.debug(
                        "Setting \"" + AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE
                                + "\" to false because the schema contains an array "
                                + "with optional items"
                );
                builder.config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false");
            }
            writer = builder.build();
        }

        Object value = avroData.fromConnectData(kafkaSchema, kafkaRecord.value());
        writer.write(value);
    }

    /**
     * To write a JSON String value, {@link AvroParquetWriter} needs an
     * Avro schema of the data. It will get the schema file path from
     * the  object and use {@link AvroSchemaStore#getSchema(String)}}
     * to get the Avro schema.
     *
     * <p>It initializes the ParquetWriter, convert the JSON string to
     * {@link GenericData.Record} and write it using the ParquetWriter.
     *
     * @param value the value of the sink record to be processed
     * @throws IOException If I/O error occur
     */
    private void write(String value) throws IOException {
        if (avroSchema == null || writer == null) {
            log.debug("Opening parquet record writer for blob: {}", blobName);

            avroSchema = (org.apache.avro.Schema) schemaStore.getSchema(topic);

            outputFile = new ParquetOutputFile(
                    this.storageManager,
                    this.blobName,
                    this.blockSize
            );
            AvroParquetWriter.Builder<GenericData.Record> builder =
                    AvroParquetWriter.<GenericData.Record>builder(outputFile)
                            .withSchema(avroSchema)
                            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                            .withDictionaryEncoding(false)
                            .withCompressionCodec(compressionCodec)
                            .withPageSize(PAGE_SIZE);

            writer = builder.build();
        }
        Object record = convertToGenericDataRecord(value);
        writer.write(record);
    }

    /**
     * It will convert the JSON String to GenericData.Record .
     *
     * @param jsonString JSON String
     * @return GenericData.Record
     */
    private GenericData.Record convertToGenericDataRecord(String jsonString) {
        if (converter == null) {
            converter = new JsonAvroConverter(); // Lazy Initialization
        }
        return converter.convertToGenericDataRecord(jsonString.getBytes(), avroSchema);
    }

    /**
     * Invoke to close the ParquetWriter. This will trigger the
     * writer to close and write the metadata at the end of the file.
     * Once closed no data can be written by this writer.
     *
     * @throws IOException If any I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    /**
     * It will set the commit flag in the ParquetOutputStream object
     * and invoke the close method of the ParquetWriter.
     * Once closed no data can be written by this writer.
     *
     * @throws IOException If any I/O error occurs
     */
    @Override
    public void commit() throws IOException {
        outputFile.getOutputStream().setCommitFlag(true);
        if (writer != null) {
            writer.close();
        }
    }

    /**
     * This will check if the schema contains an array of optional items.
     *
     * @param schema Avro schema
     * @param seenSchemas set of seen schemas
     * @return whether it has an array of optional items or not
     */
    public static boolean schemaHasArrayOfOptionalItems(Schema schema, Set<Schema> seenSchemas) {
        // First, check for infinitely recursive schemas
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

            case MAP: return schemaHasArrayOfOptionalItems(schema.valueSchema(), seenSchemas);

            case ARRAY:
                return schema.valueSchema().isOptional()
                        || schemaHasArrayOfOptionalItems(schema.valueSchema(), seenSchemas);

            default: return false;
        }
    }

    /**
     * The amount of data that has been written by the ParquetWriter till now.
     *
     * @return amount of data written so far
     */
    @Override
    public long getDataSize() {
        if (writer == null) {
            return 0L;
        }
        return this.writer.getDataSize();
    }
}
