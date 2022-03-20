package io.coffeebeans.connector.sink.partitioner.time.extractor;

import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * To extract timestamp based on the strategy configured.
 */
public interface TimestampExtractor {

    /**
     * Configure the timestamp extractor.
     *
     * @param configProps Map of configurations
     */
    void configure(Map<String, String> configProps);

    /**
     * It returns the formatted date and time.
     *
     * @param sinkRecord SinkRecord
     * @return Formatted date & time string
     */
    String getFormattedTimestamp(SinkRecord sinkRecord);
}
