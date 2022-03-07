package io.coffeebeans.connector.sink.partitioner.time;

import io.coffeebeans.connector.sink.partitioner.Partitioner;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public class DailyPartitioner implements Partitioner {
    private static final String PATH_FORMAT = "year={0}/month={1}/day={2}";

    @Override
    public void configure(Map<String, String> configProps) {

    }

    /**
     * Return file path based on today's date
     * @param sinkRecord The record to be stored
     * @param startingOffset kafka offset of the first record of the batch
     * @return <prefix>/<topic>/year='YYYY'/month='MM'/day='dd'/<topic>+<kafkaPartition>+<startOffset>.<format>
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord, long startingOffset) {
        return null;
    }
}
