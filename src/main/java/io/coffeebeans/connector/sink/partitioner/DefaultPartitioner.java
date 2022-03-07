package io.coffeebeans.connector.sink.partitioner;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public class DefaultPartitioner implements Partitioner {
    public static final String FOLDER_DELIMITER = "/";
    public static final String FILE_DELIMITER = "+";

    protected String prefix;

    private static final String kafkaPartitionProperty = "partition";

    @Override
    public void configure(Map<String, String> configProps) {
        this.prefix = configProps.get(AzureBlobSinkConfig.TOPIC_DIR);
    }

    /**
     * @param sinkRecord The record to be stored
     * @return <prefix>/<kafkaTopic>/partition=<kafkaPartition>/<kafkaTopic>+<kafkaPartition>+<startOffset>.<format>
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord, long startingOffset) {
        String kafkaTopic = sinkRecord.topic();
        long kafkaPartition = sinkRecord.kafkaPartition();

        return prefix + FOLDER_DELIMITER + // <prefix>/

                // <kafkaTopic>/
                kafkaTopic + FOLDER_DELIMITER +

                // partition=<kafkaPartition>/
                kafkaPartitionProperty + "=" + kafkaPartition + FOLDER_DELIMITER +

                // <kafkaTopic> + <kafkaPartition> + <startOffset>
                kafkaTopic + FILE_DELIMITER + kafkaPartition + FILE_DELIMITER + startingOffset;
    }
}
