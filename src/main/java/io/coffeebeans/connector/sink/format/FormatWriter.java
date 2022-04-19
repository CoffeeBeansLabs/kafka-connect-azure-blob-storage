package io.coffeebeans.connector.sink.format;

import java.io.IOException;
import java.util.Map;

/**
 * This class is used by FormatManager to buffer and write the data by the writer.
 */
public interface FormatWriter {

    /**
     * I store the writer, write using that and maintain the number of records written by the writer.
     *
     * @param data Value map
     * @throws IOException thrown if exception occur during writing the data by the writer
     */
    void write(Map<String, Object> data) throws IOException;

    /**
     * I process the written records convert it to bytes array and return it.
     *
     * @return byte array of the data
     * @throws IOException - Thrown if exception occur during processing written data
     */
    byte[] toByteArray() throws IOException;

    /**
     * I return the number of records buffered/written by the writer.
     *
     * @return Number of records written
     */
    int recordsWritten();

    void close() throws IOException;
}
