package io.coffeebeans.connector.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.storage.AzureBlobStorageManager;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AzureBlobSinkTaskTest {
    private final String topicName = "test";
    private final String containerName = "test-container";
    private final String blobIdentifierKey = "id";

    private AzureBlobStorageManager mockStorageManager;
    private AzureBlobSinkTask sinkTask;

    @BeforeEach
    public void init() throws IllegalAccessException {
        sinkTask = new AzureBlobSinkTask();
        mockStorageManager = Mockito.mock(AzureBlobStorageManager.class);

        FieldUtils.writeField(sinkTask, "containerName", containerName, true);
        FieldUtils.writeField(sinkTask, "blobIdentifierKey", blobIdentifierKey, true);
        FieldUtils.writeField(sinkTask, "objectMapper", new ObjectMapper(), true);
        FieldUtils.writeField(sinkTask, "storageManager", mockStorageManager, true);
    }

    @Test
    public void test_putString_shouldNotProcess() {
        Schema stringSchema = new ConnectSchema(Schema.Type.STRING);

        String message = "message";
        SinkRecord sinkRecord = new SinkRecord(topicName, 1, stringSchema, "nokey", stringSchema, message, 0L);
        Assertions.assertNotNull(sinkRecord.value());
        sinkTask.put(List.of(sinkRecord));
        Mockito.verify(mockStorageManager, Mockito.times(0)).upload(
                Mockito.any(), Mockito.any(), Mockito.any()
        );
    }

    @Test
    public void test_putMap_shouldProcess() throws JsonProcessingException {
        Schema stringSchema = new ConnectSchema(Schema.Type.STRING);
        Schema mapSchema = new ConnectSchema(Schema.Type.MAP);

        Map<String, String> map = new HashMap<>();
        map.put("id", "test-blob");
        map.put("key", "value");

        SinkRecord sinkRecord = new SinkRecord(topicName, 1, stringSchema, "nokey", mapSchema, map, 0L);
        Assertions.assertNotNull(sinkRecord.value());
        sinkTask.put(List.of(sinkRecord));
        Mockito.verify(mockStorageManager, Mockito.times(1)).upload(
                containerName, "test-blob", new ObjectMapper().writeValueAsBytes(sinkRecord.value())
        );
    }
}
