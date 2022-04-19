package io.coffeebeans.connector.sink.format.parquet;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import java.io.IOException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;


public class ParquetOutputFile implements OutputFile {
    private static final int DEFAULT_BLOCK_SIZE = 0;

    private final String blobName;
    private final AzureBlobSinkConfig config;
    private ParquetOutputStream outputStream;

    public ParquetOutputFile(AzureBlobSinkConfig config, String blobName) {
        this.config = config;
        this.blobName = blobName;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        outputStream = new ParquetOutputStream(config, blobName);
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
