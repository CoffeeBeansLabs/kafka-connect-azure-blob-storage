package io.coffeebeans.connector.sink.partitioner.time;

import io.coffeebeans.connector.sink.partitioner.Partitioner;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Partitioner to partition incoming records based on day.
 */
public class DailyPartitioner implements Partitioner {
    private static final String PATH_FORMAT = "year={0}/month={1}/day={2}";

    @Override
    public void configure(Map<String, String> configProps) {

    }

    /**
     * Return file path based on today's date.
     *
     * @param sinkRecord The record to be stored
     * @param startingOffset kafka offset of the first record of the batch
     * @return encoded partition string
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord, long startingOffset) {

        // Output format
        // <prefix>/<topic>/year='YYYY'/month='MM'/day='dd'/<topic>+<kafkaPartition>+<startOffset>.<format>
        return null;
    }
}
