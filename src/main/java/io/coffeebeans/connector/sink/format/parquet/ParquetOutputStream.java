package io.coffeebeans.connector.sink.format.parquet;

import io.coffeebeans.connector.sink.format.AzureBlobOutputStream;
import io.coffeebeans.connector.sink.storage.StorageManager;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used by {@link ParquetRecordWriter} to flush the data from in-memory store to some sink
 * (e.g. Files).
 */
public class ParquetOutputStream extends AzureBlobOutputStream {
    private static final Logger log = LoggerFactory.getLogger(ParquetOutputStream.class);

    private boolean commitFlag;
    private final String blobName;

    ParquetOutputStream(StorageManager storageManager, String blobName, int partSize) {
        super(storageManager, blobName, partSize);

        commitFlag = false;
        this.blobName = blobName;
    }

    /**
     * Closes this output stream and releases any system resources
     * associated with this stream. The general contract of <code>close</code>
     * is that it closes the output stream. A closed stream cannot perform
     * output operations and cannot be reopened.
     *
     * <p>This implementation will invoke the {@link #commit()}  commit()} method to
     * perform the commit and then clear and close any resource being used.
     *
     * @exception  IOException  if an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        log.debug("Close operation invoked for blob: {}", blobName);

        if (commitFlag) {
            super.commit();
            commitFlag = false;

        } else {
            super.internalClose();
        }
    }

    /**
     * Set commit flag.
     *
     * @param isCommitted Flag
     */
    public void setCommitFlag(boolean isCommitted) {
        this.commitFlag = isCommitted;
    }
}
