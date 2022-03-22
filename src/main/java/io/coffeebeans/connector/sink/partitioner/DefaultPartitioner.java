package io.coffeebeans.connector.sink.partitioner;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * The DefaultPartitioner partitions the incoming data based on the prefix, kafka topic, and kafka partition.
 */
public class DefaultPartitioner implements Partitioner {
    private static final String KAFKA_PARTITION_PROPERTY = "partition";
    public static final String FOLDER_DELIMITER = "/";
    public static final String FILE_DELIMITER = "+";

    protected String prefix;


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
        /*
          Output format:
          partition=<kafkaPartition>
         */

        return KAFKA_PARTITION_PROPERTY + "=" + sinkRecord.kafkaPartition(); // partition=<kafkaPartition>/
    }

    /**
     * Generate full blob file path including folder path with encodedPartition.
     *
     * @param sinkRecord SinkRecord
     * @param startingOffset starting offset
     * @return Full file path
     */
    @Override
    public String generateFullPath(SinkRecord sinkRecord, long startingOffset, String encodedPartition) {

        /*
          Output format:
          <prefix>/<kafkaTopic>/<encodedPartition>/<kafkaTopic>+<kafkaPartition>+<startOffset>
         */
        return generateFolderPath(sinkRecord, encodedPartition) + FOLDER_DELIMITER

                // <kafkaTopic> + <kafkaPartition> + <startOffset>
                + sinkRecord.topic() + FILE_DELIMITER + sinkRecord.kafkaPartition() + FILE_DELIMITER + startingOffset;
    }

    /**
     * I generate the folder path using the encoded partition string.
     *
     * @param sinkRecord       SinkRecord
     * @param encodedPartition Encoded partition string
     * @return Folder path
     */
    @Override
    public String generateFolderPath(SinkRecord sinkRecord, String encodedPartition) {
        /*
          Output format:
          <prefix>/<kafkaTopic>/<encodedPartition>
         */
        return prefix + FOLDER_DELIMITER // <prefix>/

                // <kafkaTopic>/
                + sinkRecord.topic() + FOLDER_DELIMITER

                // <encodedPartition>
                + encodedPartition;
    }
}
