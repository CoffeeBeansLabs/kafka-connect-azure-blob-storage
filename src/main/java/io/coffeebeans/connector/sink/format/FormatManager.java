package io.coffeebeans.connector.sink.format;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Class responsible for buffering, writing, managing metadata and uploading to storage service.
 */
public interface FormatManager {

    /**
     * I buffer the data passed as the parameter and upload it to the storage service when the buffer size/length limit
     * is met or timeout happens whichever is earlier.
     *
     * @param fullPath Full path of the blob (this includes the blob nome)
     * @param data - value map
     * @throws IOException - Thrown if exception occur during writing of record
     * @throws InterruptedException - Thrown if exception occur during updating metadata
     */
    void buffer(String fullPath, Map<String, Object> data) throws IOException, InterruptedException;

    /**
     * Get the concurrent map which maintains the full path as key and the current index of the file in which data
     * be stored.
     *
     * @return ConcurrentMap containing blob full path as key and current index as value
     */
    ConcurrentMap<String, Integer> getActiveFileIndexMap();
}
