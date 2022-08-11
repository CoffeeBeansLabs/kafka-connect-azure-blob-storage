package io.coffeebeans.connector.sink.format;

import java.io.IOException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * RecordWriter is responsible for writing the record value
 * in other file formats like (parquet, avro etc.).
 */
public interface RecordWriter {

    void write(SinkRecord sinkRecord) throws IOException;

    void close() throws IOException;

    void commit(boolean ensureCommitted) throws IOException;

    long getDataSize();
}
