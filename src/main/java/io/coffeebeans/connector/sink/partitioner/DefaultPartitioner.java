package io.coffeebeans.connector.sink.partitioner;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * The DefaultPartitioner partitions the incoming data based on the prefix, kafka topic, and kafka partition.
 */
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
     * I need the SinkRecord and the starting offset. I will create an encoded partition based on prefix, kafka topic,
     * kafka partition and starting offset.
     *
     * @param sinkRecord The record to be stored
     * @return encoded partition string
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord, long startingOffset) {
        String kafkaTopic = sinkRecord.topic();
        long kafkaPartition = sinkRecord.kafkaPartition();

        /*
          Output format:
          <prefix>/<kafkaTopic>/partition=<kafkaPartition>/<kafkaTopic>+<kafkaPartition>+<startOffset>.<format>
         */

        return prefix + FOLDER_DELIMITER // <prefix>/

                // <kafkaTopic>/
                + kafkaTopic + FOLDER_DELIMITER

                // partition=<kafkaPartition>/
                + kafkaPartitionProperty + "=" + kafkaPartition + FOLDER_DELIMITER

                // <kafkaTopic> + <kafkaPartition> + <startOffset>
                + kafkaTopic + FILE_DELIMITER + kafkaPartition + FILE_DELIMITER + startingOffset;
    }
}
