package io.coffeebeans.connector.sink.format.parquet;

import io.coffeebeans.connector.sink.format.FormatWriter;
import io.coffeebeans.connector.sink.format.FormatWriterProvider;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class store the full path, and it's respective ParquetFormatWriter.
 */
public class ParquetFormatWriterProvider implements FormatWriterProvider {
    private static final Logger logger = LoggerFactory.getLogger(FormatWriterProvider.class);

    private final Map<String, FormatWriter> parquetFormatWriters;

    /**
     * Constructor.
     */
    public ParquetFormatWriterProvider() {
        this.parquetFormatWriters = new HashMap<>();
    }

    /**
     * I take the FormatWriter instance and store it with the full path it is associated with.
     *
     * @param fullPath - Blob full path
     * @param formatWriter - FormatWriter instance to write data
     */
    @Override
    public void put(String fullPath, FormatWriter formatWriter) {
        parquetFormatWriters.put(fullPath, formatWriter);
    }

    /**
     * I return the FormatWriter instance associated with the full path passed as parameter.
     *
     * @param fullPath Full path
     * @return FormatWriter instance
     */
    @Override
    public FormatWriter get(String fullPath) {
        return parquetFormatWriters.get(fullPath);
    }

    /**
     * I remove the FormatWriter instance associated with the full path.
     *
     * @param fullPath blob full path
     */
    @Override
    public void remove(String fullPath) {
        parquetFormatWriters.remove(fullPath);
    }
}
