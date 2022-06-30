package io.coffeebeans.connector.sink.partitioner;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The DefaultPartitioner partitions the incoming data based
 * on the prefix, kafka topic, and kafka partition.
 */
public class DefaultPartitioner implements Partitioner {
    protected static final Logger log = LoggerFactory.getLogger(Partitioner.class);

    private static final String KAFKA_PARTITION_PROPERTY = "partition";
    public static final String FOLDER_DELIMITER = "/";

    private String prefix;
    private String fileDelimiter;

    public DefaultPartitioner(AzureBlobSinkConfig config) {
        this.prefix = config.getTopicsDir();
        this.fileDelimiter = config.getFileDelimiter();
    }

    /**
     * Generate an encoded partition based on prefix, kafka topic,
     * kafka partition and starting offset.
     *
     * <p>partition=&lt;kafkaPartition&gt;
     *
     * @param sinkRecord The record to be stored
     * @return encoded partition string
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord) throws JsonProcessingException {
        // partition=<kafkaPartition>/
        return KAFKA_PARTITION_PROPERTY + "=" + sinkRecord.kafkaPartition();
    }

    /**
     * Generate full blob file path including folder path with encodedPartition.
     *
     * <p>&lt;prefix&gt;/&lt;kafkaTopic&gt;/&lt;encodedPartition&gt;/&lt;kafkaTopic&gt;
     * +&lt;kafkaPartition&gt;+&lt;startOffset&gt;
     *
     * @param sinkRecord sink record to be stored
     * @param startingOffset kafka starting offset
     * @return Full file path
     */
    @Override
    public String generateFullPath(SinkRecord sinkRecord, long startingOffset) throws JsonProcessingException {
        return generateFolderPath(sinkRecord) + FOLDER_DELIMITER

                // <kafkaTopic> + <kafkaPartition> + <startOffset> + <uniqueTaskIdentifier>
                + sinkRecord.topic() + fileDelimiter + sinkRecord.kafkaPartition() + fileDelimiter + startingOffset;
    }

    /**
     * Generate full blob file path including folder path with encodedPartition.
     *
     * <p>&lt;prefix&gt;/&lt;kafkaTopic&gt;/&lt;encodedPartition&gt;/&lt;kafkaTopic&gt;
     * +&lt;kafkaPartition&gt;+&lt;startOffset&gt;
     *
     * @param sinkRecord sink record to be stored
     * @param encodedPartition encoded partition
     * @param startingOffset kafka starting offset
     * @return Full file path
     */
    @Override
    public String generateFullPath(SinkRecord sinkRecord, String encodedPartition, long startingOffset) {
        return generateFolderPath(sinkRecord, encodedPartition) + FOLDER_DELIMITER

                // <kafkaTopic> + <kafkaPartition> + <startOffset> + <uniqueTaskIdentifier>
                + sinkRecord.topic() + fileDelimiter + sinkRecord.kafkaPartition() + fileDelimiter + startingOffset;
    }

    /**
     * It generates only the folder path and does not include the file name.
     * The folder path includes encoded partition
     *
     * <p>&lt;prefix&gt;/&lt;kafkaTopic&gt;/&lt;encodedPartition&gt;
     *
     * @param sinkRecord       record to be processed
     * @return Folder path
     */
    @Override
    public String generateFolderPath(SinkRecord sinkRecord) throws JsonProcessingException {
        return prefix + FOLDER_DELIMITER // <prefix>/

                // <kafkaTopic>/
                + sinkRecord.topic() + FOLDER_DELIMITER

                // <encodedPartition>
                + encodePartition(sinkRecord);
    }

    /**
     * It generates only the folder path and does not include the file name.
     * The folder path includes encoded partition
     *
     * <p>&lt;prefix&gt;/&lt;kafkaTopic&gt;/&lt;encodedPartition&gt;
     *
     * @param sinkRecord       record to be processed
     * @param encodedPartition encoded partition
     * @return Folder path
     */
    @Override
    public String generateFolderPath(SinkRecord sinkRecord, String encodedPartition) {
        return prefix + FOLDER_DELIMITER // <prefix>/

                // <kafkaTopic>/
                + sinkRecord.topic() + FOLDER_DELIMITER

                // <encodedPartition>
                + encodedPartition;
    }

}
