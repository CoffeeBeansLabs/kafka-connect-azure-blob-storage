package io.coffeebeans.connector.sink.format.parquet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.avro.AvroSchemaBuilder;
import io.coffeebeans.connector.sink.storage.RecordWriter;
import io.confluent.connect.avro.AvroData;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;


public class ParquetRecordWriter implements RecordWriter {
    private static final Logger logger = LoggerFactory.getLogger(ParquetRecordWriter.class);
    private static final int PAGE_SIZE = 64 * 1024;
    private static final String FILE_PATH = "/usr/share/java/kafka-connect-sample/schema.avsc";

    private Schema kafkaSchema;
    private final String blobName;
    private final AvroData avroData;
    private final ObjectMapper objectMapper;
    private final AvroSchemaBuilder avroSchemaBuilder;
    private ParquetOutputFile outputFile;
    private final AzureBlobSinkConfig config;
    private org.apache.avro.Schema avroSchema;
    private ParquetWriter writer;
    private JsonAvroConverter converter = new JsonAvroConverter();

    ParquetRecordWriter(AzureBlobSinkConfig config, AvroData avroData, String blobName) {
        this.config = config;
        this.avroData = avroData;
        this.blobName = blobName;
        this.kafkaSchema = null;
        this.avroSchema = null;
        this.objectMapper = new ObjectMapper();
        this.avroSchemaBuilder = new AvroSchemaBuilder(objectMapper);
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
                        .parse(new File(FILE_PATH));
                this.avroSchema = JsonStringSchema.getSchema();

                logger.info("Parsed Schema -> ");
                logger.info(avroSchema.toString(true));

            }

//            JsonNode jsonNode = objectMapper.readTree((String) value);
//            avroSchema = new AvroSchemaBuilder(objectMapper).buildAvroSchema(jsonNode);

            outputFile = new ParquetOutputFile(config, blobName);
            AvroParquetWriter.Builder<GenericData.Record> builder = AvroParquetWriter.<GenericData.Record>builder(outputFile)
                    .withSchema(avroSchema)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withDictionaryEncoding(false)
                    .withPageSize(PAGE_SIZE);

            writer = builder.build();
        }
        Object obj = parseJsonString((String) value);
        logger.info("Generic Record \n {}", ((GenericData.Record) obj).toString());
        writer.write((GenericData.Record) obj);
    }

    private Object parseJsonString(String jsonString) throws IOException {
        try {
            //            GenericRecord record = new GenericData.Record(this.avroSchema);
            //            Map<String, Object> valueMap = objectMapper.readValue(jsonString,
            //                    new TypeReference<HashMap<String, Object>>() {});
            //
            //            valueMap.forEach(record::put);
            //            return record;

            // Approach 2
//                        JsonNode jsonNode = objectMapper.readTree(jsonString);
//                        logger.info("Generated schema ->");
//                        logger.info(new AvroSchemaBuilder(objectMapper).buildAvroSchemaAsString(jsonNode));
//                        return getOutputRecord(jsonNode, this.avroSchema);

            // Approach 3
            return converter.convertToGenericDataRecord(jsonString.getBytes(), avroSchema);

        } catch (Exception e) {
            logger.error("Failed to process record {}, with exception: {},  skipping record .......", jsonString,
                    e.getMessage());
            throw e;
        }
    }

    private GenericRecord getOutputRecord(JsonNode jsonNode, org.apache.avro.Schema avroSchema) throws JsonProcessingException {
        GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
        final Iterator<Map.Entry<String, JsonNode>> elements = jsonNode.fields();
        Map.Entry<String, JsonNode> mapEntry;

        while (elements.hasNext()) {
            mapEntry = elements.next();
            final JsonNode nextNode = mapEntry.getValue();

            if (!(nextNode instanceof NullNode)) {
                if (nextNode instanceof ValueNode) {
                    builder.set(mapEntry.getKey(), getValue(nextNode));
                } else if (nextNode instanceof ObjectNode) {
                    org.apache.avro.Schema schemaTo = avroSchemaBuilder.buildAvroSchema(nextNode);
                    GenericRecord record = getOutputRecord(nextNode, schemaTo);
                    builder.set(mapEntry.getKey(), record);
                } else  if (nextNode instanceof ArrayNode) {
                    List<Object> listRecords = new ArrayList<>();
                    Iterator<JsonNode> elementsIterator = ((ArrayNode) nextNode).elements();
                    while (elementsIterator.hasNext()) {
                        JsonNode nodeTo = elementsIterator.next();
                        if (nodeTo instanceof ValueNode) {
                            listRecords.add(getValue(nodeTo));
                        } else {
                            org.apache.avro.Schema schemaTo = avroSchemaBuilder.buildAvroSchema(nodeTo);
                            GenericRecord record = getOutputRecord(nodeTo, schemaTo);
                            listRecords.add(record);
                        }
                    }
                    builder.set(mapEntry.getKey(), listRecords);
                }
            } else {
                builder.set(mapEntry.getKey(), null);
            }
        }
        return builder.build();
    }

    private Object getValue(JsonNode node) {
        if (node instanceof TextNode) {
            return node.textValue();
        } else if (node instanceof IntNode) {
            return node.intValue();
        } else if (node instanceof LongNode) {
            return node.longValue();
        } else if (node instanceof DoubleNode) {
            return node.doubleValue();
        } else if (node instanceof BooleanNode) {
            return node.booleanValue();
        }
        return null;
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
