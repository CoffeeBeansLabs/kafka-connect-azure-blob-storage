package io.coffeebeans.connector.sink.format.parquet;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This task is launched by FormatManager and will run after delay set by the format manager.
 */
public class ParquetBufferTimeoutTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ParquetBufferTimeoutTask.class);

    private final String fullPath;
    private final ParquetFormatManager formatManager;

    /**
     * Constructor with blob full path and Format Manager from where it was scheduled as parameters.
     *
     * @param fullPath Blob full path
     * @param formatManager FormatManager
     */
    public ParquetBufferTimeoutTask(String fullPath, ParquetFormatManager formatManager) {
        this.fullPath = fullPath;
        this.formatManager = formatManager;
    }

    @Override
    public void run() {
        try {
            logger.info("Running scheduled timeout task for {}", fullPath);
            formatManager.write(fullPath, true);

        } catch (IOException | InterruptedException e) {
            logger.error("Failed to write parquet file with exception: {}", e.getMessage());
        }
    }
}
