package io.coffeebeans.connect.azure.blob.sink.partitioner.time.extractor;

/**
 * Timestamp extractor strategies supported by the connector.
 */
public enum TimestampExtractorStrategy {
    DEFAULT, RECORD, RECORD_FIELD
}
