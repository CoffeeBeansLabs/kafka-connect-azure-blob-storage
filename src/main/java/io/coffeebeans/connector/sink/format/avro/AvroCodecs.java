package io.coffeebeans.connector.sink.format.avro;

/**
 * Supported compression codecs for Avro record writer.
 */
public enum AvroCodecs {
    NULL,
    BZIP2,
    SNAPPY,
    DEFLATE
}
