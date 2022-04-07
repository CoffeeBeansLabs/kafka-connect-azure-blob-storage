package io.coffeebeans.connector.sink.format;

/**
 * This class is used by the FormatManage to get the FormatWriter instance.
 */
public interface FormatWriterProvider {

    /**
     * I take the FormatWriter instance and store it with the full path it is associated with.
     *
     * @param fullPath - Blob full path
     * @param formatWriter - FormatWriter instance to write data
     */
    void put(String fullPath, FormatWriter formatWriter);

    /**
     * I return the FormatWriter instance associated with the full path passed as parameter.
     *
     * @param fullPath Full path
     * @return FormatWriter instance
     */
    FormatWriter get(String fullPath);

    /**
     * I remove the FormatWriter instance associated with the full path.
     *
     * @param fullPath blob full path
     */
    void remove(String fullPath);
}
