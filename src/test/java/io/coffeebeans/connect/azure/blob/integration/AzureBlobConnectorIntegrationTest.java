package io.coffeebeans.connect.azure.blob.integration;

import static io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig.CONNECTION_STRING_CONF;
import static io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig.CONTAINER_NAME_CONF;
import static io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig.FLUSH_SIZE_CONF;
import static io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig.FORMAT_CONF;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.coffeebeans.connect.azure.blob.sink.AzureBlobSinkConnector;
import io.coffeebeans.connect.azure.blob.util.EmbeddedConnectUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.tools.json.JsonRecordFormatter;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("IntegrationTest")
public class AzureBlobConnectorIntegrationTest extends BaseConnectorIntegrationTest {
    public static final Logger log = LoggerFactory.getLogger(AzureBlobConnectorIntegrationTest.class);

    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static final String CONTAINER_NAME = "integration-test";

    private static final String CONNECTOR_NAME = "blob-sink";
    private static final String AVRO_EXTENSION = "avro";
    private static final String PARQUET_EXTENSION = "parquet";
    private static final String JSON_EXTENSION = "json";

    private static final String TEST_RESOURCES_PATH = "src/test/resources/";
    private static final String TEST_DOWNLOAD_PATH = TEST_RESOURCES_PATH + "downloaded-files/";

    public static final List<String> KAFKA_TOPICS = Collections.singletonList("it-test");
    private static final int NUM_RECORDS_INSERT = 30;
    private static final int FLUSH_SIZE_STANDARD = 3;
    private static final int TOPIC_PARTITION = 0;
    private static final int DEFAULT_OFFSET = 0;

    private static final Map<String, Function<String, List<JsonNode>>> contentGetters =
            ImmutableMap.of(
                    JSON_EXTENSION, AzureBlobConnectorIntegrationTest::getContentsFromJson,
                    AVRO_EXTENSION, AzureBlobConnectorIntegrationTest::getContentsFromAvro,
                    PARQUET_EXTENSION, AzureBlobConnectorIntegrationTest::getContentsFromParquet
            );

    private JsonConverter jsonConverter;
    private Producer<byte[], byte[]> producer;

    @BeforeAll
    public static void setupClient() {
        log.info("Setting up client");

        String connectionString = System.getenv(CONNECTION_STRING_ENV_VARIABLE);

        containerClient = new BlobContainerClientBuilder()
                .connectionString(connectionString)
                .containerName(CONTAINER_NAME)
                .buildClient();

        log.info("Creating container if not exist");
        createIfNotExist();
    }

    @AfterAll
    public static void deleteContainer() {
        log.info("All ITs completed, deleting the container");

        deleteIfExists();
    }

    private static void createIfNotExist() {
        if (containerClient.exists()) {
            clearContainer();
            return;
        }
        containerClient.create();
    }

    private static void deleteIfExists() {
        containerClient.deleteIfExists();
    }

    @BeforeEach
    public void before() {
        String connectionString = System.getenv(CONNECTION_STRING_ENV_VARIABLE);

        initializeJsonConverter();
        initializeCustomProducer();
        setupProperties();

        props.put(CONNECTION_STRING_CONF, connectionString);
        props.put(CONTAINER_NAME_CONF, CONTAINER_NAME);
        props.put(TOPICS_CONFIG, String.join(", ", KAFKA_TOPICS));
        props.put(FLUSH_SIZE_CONF, String.valueOf(FLUSH_SIZE_STANDARD));
        props.put(FORMAT_CONF, "AVRO");

        KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
    }

    @AfterEach
    public void after() throws Exception {
        log.info("Clearing container");

        clearContainer();
        waitForBlobsInContainer(0);
    }

    public static void clearContainer() {
        containerClient
                .listBlobs(new ListBlobsOptions(), Duration.ofMillis(AZ_BLOB_TIMEOUT_MS))
                .forEach(blob -> containerClient.getBlobClient(blob.getName())
                        .deleteIfExists());
    }

    @Test
    public void testBasicRecordsWrittenAvro() throws Throwable {

        //add test specific props
        props.put(FORMAT_CONF, "AVRO");
        testBasicRecordsWritten(AVRO_EXTENSION);
    }

    @Test
    public void testBasicRecordsWrittenParquet() throws Throwable {

        //add test specific props
        props.put(FORMAT_CONF, "PARQUET");
        testBasicRecordsWritten(PARQUET_EXTENSION);
    }

    @Test
    public void testBasicRecordsWrittenJson() throws Throwable {

        //add test specific props
        props.put(FORMAT_CONF, "JSON");
        testBasicRecordsWritten(JSON_EXTENSION);
    }

    /**
     * Initializing {@link JsonConverter}.
     */
    private void initializeJsonConverter() {
        Map<String, Object> jsonConverterProps = new HashMap<>();
        jsonConverterProps.put("schemas.enable", "true");
        jsonConverterProps.put("converter.type", "value");

        jsonConverter = new JsonConverter();
        jsonConverter.configure(jsonConverterProps);
    }

    private void setupProperties() {
        props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, AzureBlobSinkConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, Integer.toString(MAX_TASKS));

        // converters
        props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    }

    private void initializeCustomProducer() {
        Map<String, Object> producerProps = new HashMap<>();

        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());

        producer = new KafkaProducer<>(producerProps);
    }

    private void testBasicRecordsWritten(String expectedFileExtension) throws Throwable {

        Set<String> topicNames = new TreeSet<>(KAFKA_TOPICS);

        // start sink connector
        connect.configureConnector(CONNECTOR_NAME, props);

        // wait for tasks to spin up
        EmbeddedConnectUtils.waitForConnectorToStart(connect, CONNECTOR_NAME, Math.min(topicNames.size(), MAX_TASKS));

        Schema recordValueSchema = getSampleStructSchema();
        Struct recordValueStruct = getSampleStructVal(recordValueSchema);

        for (String thisTopicName : topicNames) {
            // Create and send records to Kafka using the topic name in the current 'thisTopicName'
            SinkRecord sampleRecord = getSampleTopicRecord(thisTopicName, recordValueSchema, recordValueStruct);
            produceRecordsNoHeaders(NUM_RECORDS_INSERT, sampleRecord);
        }

        log.info("Waiting for files in Blob Storage...");

        int countPerTopic = NUM_RECORDS_INSERT / FLUSH_SIZE_STANDARD;
        int expectedTotalFileCount = countPerTopic * topicNames.size();
        waitForBlobsInContainer(expectedTotalFileCount);

        Set<String> expectedTopicFilenames = new TreeSet<>();
        for (String thisTopicName : topicNames) {
            List<String> theseFiles = getExpectedBlobNames(
                    thisTopicName,
                    TOPIC_PARTITION,
                    FLUSH_SIZE_STANDARD,
                    NUM_RECORDS_INSERT,
                    expectedFileExtension
            );
            assertEquals(theseFiles.size(), countPerTopic);
            expectedTopicFilenames.addAll(theseFiles);
        }

        // This check will catch any duplications
        assertEquals(expectedTopicFilenames.size(), expectedTotalFileCount);

        // The total number of blobs allowed in the container is number of topics * # produced for each
        // All topics should have produced the same number of files, so this check should hold.
        assertBlobNamesValid(new ArrayList<>(expectedTopicFilenames));

        // Now check that all files created by the sink have the contents that were sent
        // to the producer (they're all the same content)
        assertTrue(fileContentsAsExpected(FLUSH_SIZE_STANDARD, recordValueStruct));
    }

    private Schema getSampleStructSchema() {
        return SchemaBuilder.struct()
                .field("ID", Schema.INT64_SCHEMA)
                .field("myBool", Schema.BOOLEAN_SCHEMA)
                .field("myInt32", Schema.INT32_SCHEMA)
                .field("myFloat32", Schema.FLOAT32_SCHEMA)
                .field("myFloat64", Schema.FLOAT64_SCHEMA)
                .field("myString", Schema.STRING_SCHEMA)
                .build();
    }

    private Struct getSampleStructVal(Schema structSchema) {
        Date sampleDate = new Date(1111111);
        sampleDate.setTime(0);

        return new Struct(structSchema)
                .put("ID", (long) 1)
                .put("myBool", true)
                .put("myInt32", 32)
                .put("myFloat32", 3.2f)
                .put("myFloat64", 64.64)
                .put("myString", "theStringVal");
    }

    private SinkRecord getSampleTopicRecord(String topicName, Schema recordValueSchema, Struct recordValueStruct) {
        return new SinkRecord(
                topicName,
                TOPIC_PARTITION,
                Schema.STRING_SCHEMA,
                "key",
                recordValueSchema,
                recordValueStruct,
                DEFAULT_OFFSET
        );
    }

    private void produceRecordsNoHeaders(int recordCount, SinkRecord record)
            throws ExecutionException, InterruptedException {

        produceRecords(record.topic(), recordCount, record, true, true, false);
    }

    private void produceRecords(String topic,
                                int recordCount,
                                SinkRecord record,
                                boolean withKey,
                                boolean withValue,
                                boolean withHeaders) throws ExecutionException, InterruptedException {

        byte[] kafkaKey = null;
        byte[] kafkaValue = null;

        Iterable<Header> headers = Collections.emptyList();
        if (withKey) {
            kafkaKey = jsonConverter.fromConnectData(topic, Schema.STRING_SCHEMA, record.key());
        }
        if (withValue) {
            kafkaValue = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        }
        if (withHeaders) {
            headers = sampleHeaders();
        }
        ProducerRecord<byte[],byte[]> producerRecord =
                new ProducerRecord<>(topic, TOPIC_PARTITION, kafkaKey, kafkaValue, headers);

        for (long i = 0; i < recordCount; i++) {
            producer.send(producerRecord).get();
        }
    }

    private Iterable<Header> sampleHeaders() {
        return Arrays.asList(
                new RecordHeader("first-header-key", "first-header-value".getBytes()),
                new RecordHeader("second-header-key", "second-header-value".getBytes())
        );
    }

    private static List<JsonNode> getContentsFromJson(String filePath) {

        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            List<JsonNode> fileRows = new ArrayList<>();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                fileRows.add(jsonMapper.readTree(line));
            }
            return fileRows;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<JsonNode> getContentsFromAvro(String filePath) {
        try {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader(new File(filePath),
                    datumReader);
            List<JsonNode> fileRows = new ArrayList<>();
            while (dataFileReader.hasNext()) {
                GenericRecord row = dataFileReader.next();
                JsonNode jsonNode = jsonMapper.readTree(row.toString());
                fileRows.add(jsonNode);
            }
            return fileRows;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<JsonNode> getContentsFromParquet(String filePath) {
        try {
            ParquetReader<SimpleRecord> reader = ParquetReader
                    .builder(new SimpleReadSupport(), new Path(filePath)).build();
            ParquetMetadata metadata = ParquetFileReader
                    .readFooter(new Configuration(), new Path(filePath));
            JsonRecordFormatter.JsonGroupFormatter formatter = JsonRecordFormatter
                    .fromSchema(metadata.getFileMetaData().getSchema());
            List<JsonNode> fileRows = new ArrayList<>();
            for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
                JsonNode jsonNode = jsonMapper.readTree(formatter.formatRecord(value));
                fileRows.add(jsonNode);
            }
            return fileRows;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean fileContentsAsExpected(int expectedRowsPerFile,
                                           Struct expectedRow) throws IOException {

        log.info("expectedRow: {}", expectedRow);

        PagedIterable<BlobItem> blobs = containerClient.listBlobs();

        for (BlobItem blob : blobs) {

            String blobName = blob.getName();
            String destinationPath = TEST_DOWNLOAD_PATH + blobName;

            File downloadedFile = new File(destinationPath);
            FileUtils.touch(downloadedFile);

            log.info("Saving file to : {}", destinationPath);
            containerClient.getBlobClient(blobName)
                            .downloadToFile(destinationPath, true);

            String fileExtension = getBlobExtension(blobName);

            List<JsonNode> downloadedFileContents = contentGetters.get(fileExtension)
                    .apply(destinationPath);

            if (!fileContentsMatchExpected(downloadedFileContents, expectedRowsPerFile, expectedRow)) {
                return false;
            }

            downloadedFile.delete();
            FileUtils.deleteDirectory(new File(TEST_DOWNLOAD_PATH));
        }
        return true;
    }

    private boolean fileContentsMatchExpected(List<JsonNode> fileContents,
                                              int expectedRowsPerFile,
                                              Struct expectedRow) {

        if (fileContents.size() != expectedRowsPerFile) {
            log.error("Number of rows in file do not match the expected count, actual: {}, expected: {}",
                    fileContents.size(), expectedRowsPerFile);
            return false;
        }
        for (JsonNode row : fileContents) {
            if (!fileRowMatchesExpectedRow(row, expectedRow)) {
                return false;
            }
        }
        return true;
    }

    private boolean fileRowMatchesExpectedRow(JsonNode fileRow, Struct expectedRow) {

        log.debug("Comparing rows: file: {}, expected: {}", fileRow, expectedRow);
        // compare the field values
        for (Field key : expectedRow.schema().fields()) {
            String expectedValue = expectedRow.get(key).toString();
            String rowValue = fileRow.get(key.name()).toString().replaceAll("^\"|\"$", "");
            log.debug("Comparing values: {}, {}", expectedValue, rowValue);
            if (!rowValue.equals(expectedValue)) {
                return false;
            }
        }
        return true;
    }

    private static String getBlobExtension(String blobName) {

        int lastDot = blobName.lastIndexOf('.');
        if (lastDot < 0) {
            // no extension
            throw new RuntimeException("Could not parse extension from blob name: " + blobName);
        }

        return blobName.substring(lastDot + 1);
    }
}
