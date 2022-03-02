package io.coffeebeans.connector.sink.partitioner;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public class DefaultPartitioner implements Partitioner {
    private String topicDir;
    private static final String kafkaPartitionProperty = "partition=";

    @Override
    public void configure(Map<String, String> configProps) {
        this.topicDir = configProps.get(AzureBlobSinkConfig.TOPIC_DIR);
    }

    /**
     * @param sinkRecord The record to be stored
     * @return <prefix>/<topic>/partition=<kafkaPartition>/<topic>+<kafkaPartition>+<startOffset>.<format>
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord, long startingOffset) {
        String topic = sinkRecord.topic();
        long partition = sinkRecord.kafkaPartition();

        return topicDir + folderDelimiter +
                topic + folderDelimiter +
                kafkaPartitionProperty + partition + folderDelimiter +
                topic + fileDelimiter + partition + fileDelimiter + startingOffset +
                "." + format;
    }
}
