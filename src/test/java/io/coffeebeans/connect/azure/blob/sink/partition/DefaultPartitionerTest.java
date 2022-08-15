package io.coffeebeans.connect.azure.blob.sink.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connect.azure.blob.sink.partitioner.DefaultPartitioner;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;


/**
 * Unit tests for {@link DefaultPartitionerTest}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DefaultPartitionerTest {

    @Mock
    private AzureBlobSinkConfig config;

    private DefaultPartitioner partitioner;

    /**
     * Init.
     */
    @BeforeEach
    public void init() {

        when(config.getTopicsDir())
                .thenReturn("test");

        when(config.getDirectoryDelim())
                .thenReturn("/");

        when(config.getFileDelim())
                .thenReturn("+");

        partitioner = new DefaultPartitioner(config);
    }

    /**
     * <b>Method name:
     * {@link DefaultPartitioner#encodePartition(SinkRecord)
     * encodePartition(SinkRecord)}</b>.<br>
     *
     * <b>Expectations: </b>
     * <ul>
     *     <li>Should return encoded partition</li>
     * </ul>
     */
    @Test
    @DisplayName("Given sink record, should return encoded partition string")
    void encodePartition_givenSinkRecord_shouldReturnEncodedPartitionString() throws JsonProcessingException {

        SinkRecord record = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 3L
        );

        String expectedEncodedPartition = "partition=0";
        String actualEncodedPartition = partitioner.encodePartition(record);

        assertEquals(expectedEncodedPartition, actualEncodedPartition);
    }

    /**
     * <b>Method name:
     * {@link DefaultPartitioner#generateFullPath(SinkRecord, long)}
     * generateFullPath(SinkRecord, long)}</b>.<br>
     *
     * <b>Expectations: </b>
     * <ul>
     *     <li>Should generate full path</li>
     * </ul>
     */
    @Test
    @DisplayName("Given sink record, should generate full path")
    void generateFullPath_givenSinkRecord_shouldGenerateFullPath() throws JsonProcessingException {

        SinkRecord record = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 3L
        );

        String expectedFullPath = "test/TEST-TOPIC/partition=0/TEST-TOPIC+0+0";
        String actualFullPath = partitioner.generateFullPath(record, 0L);

        assertEquals(expectedFullPath, actualFullPath);
    }

    /**
     * <b>Method name:
     * {@link DefaultPartitioner#generateFullPath(SinkRecord, String, long)}
     * generateFullPath(SinkRecord, String, long)}</b>.<br>
     * <b>Assumption: </b>
     * <ul>
     *     <li>Encoded partition is given</li>
     * </ul>
     *
     * <b>Expectations: </b>
     * <ul>
     *     <li>Should generate full path</li>
     * </ul>
     */
    @Test
    @DisplayName("Given sink record and encoded partition, should generate full path")
    void generateFullPath_givenSinkRecordAndEncodedPartition_shouldGenerateFullPath() throws JsonProcessingException {

        SinkRecord record = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 3L
        );

        String encodedPartition = "given=partition";
        String expectedFullPath = "test/TEST-TOPIC/given=partition/TEST-TOPIC+0+0";
        String actualFullPath = partitioner.generateFullPath(record, encodedPartition, 0L);

        assertEquals(expectedFullPath, actualFullPath);
    }
}
