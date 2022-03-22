package io.coffeebeans.connector.sink.partitioner;

import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Partitioner to partition the incoming fields based on the strategy configured.
 */
public interface Partitioner {

    /**
     * Configure the partitioner.
     *
     * @param configProps configuration parameters like partition strategy, etc.
     */
    void configure(Map<String, String> configProps);

    /**
     * Returns the string representing path where it will be stored.
     *
     * @param sinkRecord The record to be stored
     * @return The path where it will be stored
     */
    String encodePartition(SinkRecord sinkRecord, long startingOffset);

    /**
     * Generate full blob file path including folder path with encodedPartition.
     *
     * @param sinkRecord SinkRecord
     * @param startingOffset starting offset
     * @param encodedPartition Encoded partition string
     * @return Full file path
     */
    String generateFullPath(SinkRecord sinkRecord, long startingOffset, String encodedPartition);

    /**
     * I generate the folder path using the encoded partition string.
     *
     * @param sinkRecord SinkRecord
     * @param encodedPartition Encoded partition string
     * @return Folder path
     */
    String generateFolderPath(SinkRecord sinkRecord, String encodedPartition);
}
