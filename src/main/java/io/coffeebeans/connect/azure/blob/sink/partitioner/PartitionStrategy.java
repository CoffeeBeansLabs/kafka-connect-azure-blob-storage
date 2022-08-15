package io.coffeebeans.connect.azure.blob.sink.partitioner;

/**
 * Partition strategies supported by the connector.
 */
public enum PartitionStrategy {
    DEFAULT, FIELD, TIME
}
