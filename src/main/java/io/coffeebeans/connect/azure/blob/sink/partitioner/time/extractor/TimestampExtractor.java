package io.coffeebeans.connect.azure.blob.sink.partitioner.time.extractor;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * To extract timestamp based on the strategy configured.
 */
public interface TimestampExtractor {

    /**
     * It returns the formatted date and time.
     *
     * @param sinkRecord SinkRecord
     * @return Formatted date & time string
     */
    String getFormattedTimestamp(SinkRecord sinkRecord) throws JsonProcessingException;
}
