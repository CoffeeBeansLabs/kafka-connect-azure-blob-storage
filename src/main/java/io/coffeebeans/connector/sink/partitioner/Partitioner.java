package io.coffeebeans.connector.sink.partitioner;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public interface Partitioner {
    String folderDelimiter = "/";
    String fileDelimiter = "+";
    String format = "json";

    /**
     * Configure the partitioner
     * @param configProps configuration parameters like partition strategy, etc.
     */
    void configure(Map<String, String> configProps);

    /**
     * Returns the string representing path where it will be stored.
     * @param sinkRecord The record to be stored
     * @return The path where it will be stored
     */
    String encodePartition(SinkRecord sinkRecord, long startingOffset);
}
