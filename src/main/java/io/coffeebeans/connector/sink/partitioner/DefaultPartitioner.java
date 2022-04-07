package io.coffeebeans.connector.sink.partitioner;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The DefaultPartitioner partitions the incoming data based on the prefix, kafka topic, and kafka partition.
 */
public class DefaultPartitioner implements Partitioner {
    protected static final Logger logger = LoggerFactory.getLogger(Partitioner.class);

    private static final String KAFKA_PARTITION_PROPERTY = "partition";
    public static final String FOLDER_DELIMITER = "/";
    public static final String FILE_DELIMITER = "+";

    protected long startingOffset = -1;
    protected String prefix;

    public DefaultPartitioner(AzureBlobSinkConfig config) {
        this.prefix = config.getTopicsDir();
    }

    /**
     * I need the SinkRecord and the starting offset. I will create an encoded partition based on prefix, kafka topic,
     * kafka partition and starting offset.
     *
     * @param sinkRecord The record to be stored
     * @return encoded partition string partition=&lt;kafkaPartition&gt;
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord) {
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
     * @return Full file path; &lt;prefix&gt;/&lt;kafkaTopic&gt;/&lt;encodedPartition&gt;/
     *      &lt;kafkaTopic&gt;+&lt;kafkaPartition&gt;+&lt;startOffset&gt;
     */
    @Override
    public String generateFullPath(SinkRecord sinkRecord) {
        setStartingOffset(sinkRecord.kafkaOffset());

        /*
          Output format:
          <prefix>/<kafkaTopic>/<encodedPartition>/<kafkaTopic>+<kafkaPartition>+<startOffset>
         */
        return generateFolderPath(sinkRecord) + FOLDER_DELIMITER

                // <kafkaTopic> + <kafkaPartition> + <startOffset>
                + sinkRecord.topic() + FILE_DELIMITER + sinkRecord.kafkaPartition() + FILE_DELIMITER + startingOffset;
    }

    /**
     * I generate the folder path using the encoded partition string.
     *
     * @param sinkRecord       SinkRecord
     * @return Folder path
     */
    @Override
    public String generateFolderPath(SinkRecord sinkRecord) {
        /*
          Output format:
          <prefix>/<kafkaTopic>/<encodedPartition>
         */
        return prefix + FOLDER_DELIMITER // <prefix>/

                // <kafkaTopic>/
                + sinkRecord.topic() + FOLDER_DELIMITER

                // <encodedPartition>
                + encodePartition(sinkRecord);
    }

    private void setStartingOffset(long offset) {
        if (startingOffset < 0) {
            startingOffset = offset;

            logger.debug("Starting offset set to {}", startingOffset);
        }
    }
}
