package io.coffeebeans.connect.azure.blob.sink.format;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * RecordWriter is responsible for writing the record value
 * in other file formats like (parquet, avro etc.).
 */
public interface RecordWriter {

    void write(SinkRecord sinkRecord) throws RetriableException;

    void close() throws RetriableException;

    void commit() throws RetriableException;
}
