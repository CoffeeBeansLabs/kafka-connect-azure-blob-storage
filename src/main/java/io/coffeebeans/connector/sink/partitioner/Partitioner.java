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
}
