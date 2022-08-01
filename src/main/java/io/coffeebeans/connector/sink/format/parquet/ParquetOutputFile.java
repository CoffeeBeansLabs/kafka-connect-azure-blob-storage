package io.coffeebeans.connector.sink.format.parquet;

import io.coffeebeans.connector.sink.storage.StorageManager;
import java.io.IOException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * This will be used by the ParquetWriter to create
 * PositionOutputStream and flush the data from in-memory
 * store to some sink.
 *
 * <p>This implementation of OutputFile will create instance
 * of {@link ParquetOutputStream} to store data in sink.
 */
public class ParquetOutputFile implements OutputFile {
    private static final int DEFAULT_BLOCK_SIZE = 0;

    private final ParquetOutputStream outputStream;

    public ParquetOutputFile(StorageManager storageManager, String blobName, int partSize) {
        outputStream = new ParquetOutputStream(storageManager, blobName, partSize);
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return outputStream;
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return DEFAULT_BLOCK_SIZE;
    }

    public ParquetOutputStream getOutputStream() {
        return this.outputStream;
    }
}
