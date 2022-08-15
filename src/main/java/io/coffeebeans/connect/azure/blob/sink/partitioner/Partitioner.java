package io.coffeebeans.connect.azure.blob.sink.partitioner;

import io.coffeebeans.connect.azure.blob.sink.exception.PartitionException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Partitioner to partition the incoming fields based on the strategy configured.
 */
public interface Partitioner {

    /**
     * Returns the string representing path where it will be stored.
     * Encoded partition is dependent on the partition strategy.
     *
     * <p>For e.g. partition=&lt;kafkaPartition&gt;
     *
     * @param sinkRecord The record to be stored
     * @return encoded partition
     */
    String encodePartition(SinkRecord sinkRecord) throws PartitionException;

    /**
     * Generate file name prefixed with full path (directory info.) including
     * encoded partition.
     *
     * <p>&lt;prefix&gt;/&lt;kafkaTopic&gt;/&lt;encodedPartition&gt;/&lt;kafkaTopic&gt;
     * +&lt;kafkaPartition&gt;+&lt;startOffset&gt;
     *
     * @param sinkRecord The record to be stored
     * @param startingOffset The kafka starting offset
     * @return file name prefixed with its full path
     */
    String generateFullPath(SinkRecord sinkRecord, long startingOffset) throws PartitionException;

    /**
     * Generate file name prefixed with full path (directory info.) including
     * encoded partition.
     *
     * <p>&lt;prefix&gt;/&lt;kafkaTopic&gt;/&lt;encodedPartition&gt;/&lt;kafkaTopic&gt;
     * +&lt;kafkaPartition&gt;+&lt;startOffset&gt;
     *
     * @param sinkRecord The record to be stored
     * @param encodedPartition encoded partition
     * @param startingOffset The kafka starting offset
     * @return file name prefixed with its full path
     */
    String generateFullPath(SinkRecord sinkRecord, String encodedPartition, long startingOffset);

    /**
     * It generates only the folder path and does not include the file name.
     * The folder path includes encoded partition
     *
     * <p>&lt;prefix&gt;/&lt;kafkaTopic&gt;/&lt;encodedPartition&gt;
     *
     * @param sinkRecord The record to be stored
     * @return Folder path
     * @throws PartitionException If any processing exception occurs
     */
    String generateFolderPath(SinkRecord sinkRecord) throws PartitionException;

    /**
     * It generates only the folder path and does not include the file name.
     * The folder path includes encoded partition
     *
     * <p>&lt;prefix&gt;/&lt;kafkaTopic&gt;/&lt;encodedPartition&gt;
     *
     * @param sinkRecord The record to be stored
     * @param encodedPartition encoded partition string
     * @return Folder path
     */
    String generateFolderPath(SinkRecord sinkRecord, String encodedPartition);
}
