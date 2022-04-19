package io.coffeebeans.connector.sink.storage;

import java.io.IOException;
import org.apache.kafka.connect.sink.SinkRecord;


public interface RecordWriter {

    void write(SinkRecord sinkRecord) throws IOException;

    void close() throws IOException;

    void commit() throws IOException;

    long getDataSize();
}
