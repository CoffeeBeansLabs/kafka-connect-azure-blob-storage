package io.coffeebeans.connect.azure.blob.sink.partitioner;

import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
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

    protected String prefix;
    public String fileDelim;
    public String directoryDelim;

    /**
     * Constructs {@link DefaultPartitioner}.
     *
     * @param config Connector configuration
     */
    public DefaultPartitioner(AzureBlobSinkConfig config) {

        this.prefix = config.getTopicsDir();
        this.fileDelim = config.getFileDelim();
        this.directoryDelim = config.getDirectoryDelim();
    }

    /**
     * Generate an encoded partition based on prefix, kafka topic,
     * kafka partition and starting offset.
     * <pre>
     *     <code>
     *
     *          partition=&lt;kafkaPartition&gt;
     *     </code>
     * </pre>
     *
     * @param sinkRecord The record to be stored
     * @return encoded partition string
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        // partition=<kafkaPartition>/
        return KAFKA_PARTITION_PROPERTY + "=" + sinkRecord.kafkaPartition();
    }

    /**
     * Generate full blob file path including folder path with encodedPartition.
     * <pre>
     *     <code>
     *
     * &lt;prefix&gt;/&lt;kafkaTopic&gt;/&lt;encodedPartition&gt;/&lt;kafkaTopic&gt;
     *         +&lt;kafkaPartition&gt;+&lt;startOffset&gt;
     *     </code>
     * </pre>
     *
     *
     * @param sinkRecord sink record to be stored
     * @param startingOffset kafka starting offset
     * @return Full file path
     */
    @Override
    public String generateFullPath(SinkRecord sinkRecord, long startingOffset) {
        return generateFolderPath(sinkRecord) + directoryDelim

                // <kafkaTopic> + <kafkaPartition> + <startOffset> + <uniqueTaskIdentifier>
                + sinkRecord.topic() + fileDelim + sinkRecord.kafkaPartition() + fileDelim + startingOffset;
    }

    /**
     * Generate full blob file path including folder path with encodedPartition.
     * <pre>
     *     <code>
     *
     * &lt;prefix&gt;/&lt;kafkaTopic&gt;/&lt;encodedPartition&gt;/&lt;kafkaTopic&gt;
     *          +&lt;kafkaPartition&gt;+&lt;startOffset&gt;
     *     </code>
     * </pre>
     *
     * @param sinkRecord sink record to be stored
     * @param encodedPartition encoded partition
     * @param startingOffset kafka starting offset
     * @return Full file path
     */
    @Override
    public String generateFullPath(SinkRecord sinkRecord, String encodedPartition, long startingOffset) {
        return generateFolderPath(sinkRecord, encodedPartition) + directoryDelim

                // <kafkaTopic> + <kafkaPartition> + <startOffset> + <uniqueTaskIdentifier>
                + sinkRecord.topic() + fileDelim + sinkRecord.kafkaPartition() + fileDelim + startingOffset;
    }

    /**
     * It generates only the folder path and does not include the file name.
     * The folder path includes encoded partition
     *
     * <pre>
     *     <code>
     *
     *         &lt;prefix&gt;/&lt;kafkaTopic&gt;/&lt;encodedPartition&gt;
     *     </code>
     * </pre>
     *
     * @param sinkRecord       record to be processed
     * @return Folder path
     */
    @Override
    public String generateFolderPath(SinkRecord sinkRecord) {
        return prefix + directoryDelim // <prefix>/

                // <kafkaTopic>/
                + sinkRecord.topic() + directoryDelim

                // <encodedPartition>
                + encodePartition(sinkRecord);
    }

    /**
     * It generates only the folder path and does not include the file name.
     * The folder path includes encoded partition
     * <pre>
     *     <code>
     *
     *         &lt;prefix&gt;/&lt;kafkaTopic&gt;/&lt;encodedPartition&gt;
     *     </code>
     * </pre>
     *
     * @param sinkRecord       record to be processed
     * @param encodedPartition encoded partition
     * @return Folder path
     */
    @Override
    public String generateFolderPath(SinkRecord sinkRecord, String encodedPartition) {
        return prefix + directoryDelim // <prefix>/

                // <kafkaTopic>/
                + sinkRecord.topic() + directoryDelim

                // <encodedPartition>
                + encodedPartition;
    }

}
