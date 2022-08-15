package io.coffeebeans.connect.azure.blob.sink.format.parquet;

import io.coffeebeans.connect.azure.blob.sink.storage.StorageManager;
import java.io.IOException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * Used by {@link ParquetRecordWriter} to create<br>
 * PositionOutputStream and flush the data from in-memory<br>
 * store to some sink.
 *
 * <p>This implementation of {@link OutputFile} will create instance<br>
 * of {@link ParquetOutputStream} to store data in sink.
 */
public class ParquetOutputFile implements OutputFile {
    private static final int DEFAULT_BLOCK_SIZE = 0;

    private final ParquetOutputStream outputStream;

    /**
     * Constructs {@link ParquetOutputFile}.
     *
     * @param storageManager Storage manager to interact with Azure blob storage
     * @param blobName Blob name
     * @param blockSize Block size
     */
    public ParquetOutputFile(StorageManager storageManager, String blobName, int blockSize) {
        outputStream = new ParquetOutputStream(storageManager, blobName, blockSize);
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

    /**
     * Returns {@link ParquetOutputStream}.
     *
     * @return OutputStream
     */
    public ParquetOutputStream getOutputStream() {
        return this.outputStream;
    }
}
