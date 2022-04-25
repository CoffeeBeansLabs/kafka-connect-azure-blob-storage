package io.coffeebeans.connector.sink.format.parquet;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.avro.AvroSchemaStore;
import io.confluent.connect.avro.AvroData;
import java.io.IOException;
import java.util.HashSet;
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
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

/**
 * ParquetRecordWriter is used to write Parquet files.
 * It can process Kafka Struct, JSON String and value Map.
 */
public class ParquetRecordWriter implements RecordWriter {
    private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriter.class);
    private static final int PAGE_SIZE = 64 * 1024;

    private Schema kafkaSchema;
    private ParquetWriter writer;
    private final String blobName;
    private final AvroData avroData;
    private JsonAvroConverter converter;
    private ParquetOutputFile outputFile;
    private final AzureBlobSinkConfig config;
    private org.apache.avro.Schema avroSchema;

    /**
     * Constructor.
     *
     * @param config Connector config object
     * @param avroData Avro data containing configuration properties
     * @param blobName blob name (prefixed with directory info and suffixed with file extension)
     */
    public ParquetRecordWriter(AzureBlobSinkConfig config, AvroData avroData, String blobName) {
        this.config = config;
        this.avroData = avroData;
        this.blobName = blobName;
        this.kafkaSchema = null;
        this.avroSchema = null;
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
     * @param sinkRecord sink record to be processed
     * @throws IOException if any I/O error occur
     */
    @Override
    public void write(SinkRecord sinkRecord) throws IOException {
        if (sinkRecord.value() instanceof String) {
            writeFromJsonString(sinkRecord.value());
            return;
        }

        if (kafkaSchema == null || writer == null) {
            log.info("Opening parquet record writer for blob: {}", blobName);

            kafkaSchema = sinkRecord.valueSchema();
            org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(kafkaSchema);

            outputFile = new ParquetOutputFile(blobName, config.getPartSize());
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
                log.debug(
                        "Setting \"" + AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE
                                + "\" to false because the schema contains an array "
                                + "with optional items"
                );
                builder.config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false");
            }
            writer = builder.build();
        }

        Object value = avroData.fromConnectData(kafkaSchema, sinkRecord.value());
        writer.write(value);
    }

    /**
     * To write a JSON String value, {@link AvroParquetWriter} needs an
     * Avro schema of the data. It will get the schema file path from
     * the {@link #config} object and use {@link AvroSchemaStore#loadFromFile(String)}
     * to load, parse and save the Avro schema.
     *
     * <p>It initializes the ParquetWriter, convert the JSON string to
     * {@link GenericData.Record} and write it using the ParquetWriter.
     *
     * @param value the value of the sink record to be processed
     * @throws IOException If I/O error occur
     */
    private void writeFromJsonString(Object value) throws IOException {
        if (avroSchema == null || writer == null) {
            log.info("Opening parquet record writer for blob: {}", blobName);

            if (AvroSchemaStore.get() == null) {
                AvroSchemaStore.loadFromFile(config.getSchemaFile());
            }
            avroSchema = AvroSchemaStore.get();

            outputFile = new ParquetOutputFile(blobName, config.getPartSize());
            AvroParquetWriter.Builder<GenericData.Record> builder =
                    AvroParquetWriter.<GenericData.Record>builder(outputFile)
                    .withSchema(avroSchema)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withDictionaryEncoding(false)
                    .withPageSize(PAGE_SIZE);

            writer = builder.build();
        }
        Object record = convertToGenericDataRecord((String) value);
        writer.write(record);
    }

    /**
     * It will convert the JSON String to GenericData.Record .
     *
     * @param jsonString JSON String
     * @return GenericData.Record
     */
    private GenericData.Record convertToGenericDataRecord(String jsonString) {
        try {
            if (converter == null) {
                converter = new JsonAvroConverter(); // Lazy Initialization
            }
            return converter.convertToGenericDataRecord(jsonString.getBytes(), avroSchema);
        } catch (Exception e) {
            log.error("Failed to process record {}, with exception: {}", jsonString, e.getMessage());
            throw e;
        }
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
        outputFile.getOutputStream().setCommit(true);
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
          case MAP:
              return schemaHasArrayOfOptionalItems(schema.valueSchema(), seenSchemas);
          case ARRAY:
              return schema.valueSchema().isOptional()
                      || schemaHasArrayOfOptionalItems(schema.valueSchema(), seenSchemas);
          default:
              return false;
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
