package io.coffeebeans.connector.sink.partitioner;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Partitioner to partition the incoming fields based on the strategy configured.
 */
public interface Partitioner {

    /**
     * Returns the string representing path where it will be stored.
     *
     * @param sinkRecord The record to be stored
     * @return The path where it will be stored
     */
    String encodePartition(SinkRecord sinkRecord) throws JsonProcessingException;

    /**
     * Generate full blob file path including folder path with encodedPartition.
     *
     * @param sinkRecord SinkRecord
     * @return Full file path
     */
    String generateFullPath(SinkRecord sinkRecord) throws JsonProcessingException;

    /**
     * I generate the folder path using the encoded partition string.
     *
     * @param sinkRecord SinkRecord
     * @return Folder path
     */
    String generateFolderPath(SinkRecord sinkRecord) throws JsonProcessingException;
}
