package io.coffeebeans.connector.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connector.sink.partitioner.Partitioner;
import io.coffeebeans.connector.sink.storage.AzureBlobStorageManager;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AzureBlobSinkTaskTest {
    // CONFIGURATION PROPS
    private static final String CONNECTION_STRING = "AccountName=devstoreaccount1;" +
            "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K" +
            "1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://" +
            "host.docker.internal:10000/devstoreaccount1;";
    private static final String CONTAINER_NAME = "containerName";
    private static final String BLOB_IDENTIFIER_KEY = "blobIdentifierKey";
    private static final String TOPIC_DIR = "test";
    private static final String PARTITION_STRATEGY = "DEFAULT";

    // FIELD NAMES
    private static final String FIELD_STARTING_OFFSET = "startingOffset";
    private static final String FIELD_PARTITIONER = "partitioner";
    private static final String FIELD_OBJECT_MAPPER = "objectMapper";
    private static final String FIELD_STORAGE_MANAGER = "storageManager";

    // FIELD VALUES
    private static final String STARTING_OFFSET = "0";
    private static final Partitioner PARTITIONER = new DefaultPartitioner();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // KAFKA PROPERTIES
    private static final String KAFKA_TOPIC_NAME = "test";
    private static final int KAFKA_PARTITION = 0;
    private static final String KAFKA_RECORD_KEY = "nokey";
    private static final long KAFKA_OFFSET = 0L;

    private AzureBlobStorageManager mockStorageManager;
    private AzureBlobSinkTask sinkTask;

//    @BeforeEach
    public void init() throws IllegalAccessException {
        sinkTask = new AzureBlobSinkTask();
        mockStorageManager = Mockito.mock(AzureBlobStorageManager.class);

        Map<String, String> configProps = new HashMap<>();
        configProps.put(AzureBlobSinkConfig.CONN_STRING_CONF, CONNECTION_STRING);
        configProps.put(AzureBlobSinkConfig.CONTAINER_NAME_CONF, CONTAINER_NAME);
//        configProps.put(AzureBlobSinkConfig.BLOB_IDENTIFIER_KEY, BLOB_IDENTIFIER_KEY);
        configProps.put(AzureBlobSinkConfig.TOPIC_DIR, TOPIC_DIR);
        configProps.put(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF, PARTITION_STRATEGY);

        sinkTask.start(configProps);

        FieldUtils.writeField(sinkTask, FIELD_PARTITIONER, PARTITIONER, true);
        FieldUtils.writeField(sinkTask, FIELD_OBJECT_MAPPER, OBJECT_MAPPER, true);
        FieldUtils.writeField(sinkTask, FIELD_STORAGE_MANAGER, mockStorageManager, true);
    }

//    @Test
//    public void test_putString_shouldNotProcess() throws IllegalAccessException {
//        init(); // Initialize SinkTask
//        Schema stringSchema = new ConnectSchema(Schema.Type.STRING);
//        String message = "message";
//
//        // Create sink record
//        SinkRecord sinkRecord = new SinkRecord(
//                KAFKA_TOPIC_NAME,
//                KAFKA_PARTITION,
//                stringSchema,
//                KAFKA_RECORD_KEY,
//                stringSchema,
//                message,
//                KAFKA_OFFSET);
//
//        // Assert sink record is not null
//        Assertions.assertNotNull(sinkRecord.value());
//
//        sinkTask.put(List.of(sinkRecord));
//
//        // Verify Storage manager is not invoked
//        Mockito.verify(mockStorageManager, Mockito.times(0)).upload(
//                Mockito.any(), Mockito.any(), Mockito.any()
//        );
//    }

//    @Test
//    public void test_putMap_shouldProcess() throws JsonProcessingException, IllegalAccessException {
//        init();
//        Partitioner partitioner = new DefaultPartitioner();
//        Schema stringSchema = new ConnectSchema(Schema.Type.STRING);
//        Schema mapSchema = new ConnectSchema(Schema.Type.MAP);
//
//        Map<String, String> map = new HashMap<>();
//        map.put("id", "test-blob");
//        map.put("key", "value");
//
//        SinkRecord sinkRecord = new SinkRecord(
//                KAFKA_TOPIC_NAME,
//                KAFKA_PARTITION,
//                stringSchema,
//                KAFKA_RECORD_KEY,
//                mapSchema,
//                map,
//                KAFKA_OFFSET);
//
//        Assertions.assertNotNull(sinkRecord.value());
//
//        sinkTask.put(List.of(sinkRecord));
//
//        String expectedBlobName = partitioner.encodePartition(sinkRecord, KAFKA_OFFSET);
//
//        Mockito.verify(mockStorageManager, Mockito.times(1)).upload(
//                CONTAINER_NAME, expectedBlobName, OBJECT_MAPPER.writeValueAsBytes(sinkRecord.value())
//        );
//    }
}
