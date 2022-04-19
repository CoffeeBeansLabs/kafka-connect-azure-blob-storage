package io.coffeebeans.connector.sink.format.parquet;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.storage.StorageFactory;
import io.coffeebeans.connector.sink.storage.StorageManager;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ParquetOutputStream extends PositionOutputStream {
    private static final Logger logger = LoggerFactory.getLogger(ParquetOutputStream.class);

    private Long position;
    private boolean closed;
    private boolean commit;
    private final int partSize;
    private final String blobName;
    private final ByteBuffer buffer;
    private final StorageManager storageManager;

    public ParquetOutputStream(AzureBlobSinkConfig config, String blobName) {
        this.position = 0L;
        this.closed = false;
        this.commit = false;
        this.blobName = blobName;
        this.partSize = config.getPartSize();
        this.buffer = ByteBuffer.allocate(this.partSize);
        this.storageManager = StorageFactory.getStorageManager();

        logger.info("Configured parquet output stream with part size: {}, for {}", this.partSize, blobName);
    }

    public void setCommit(boolean commit) {
        this.commit = commit;
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public void write(int b) throws IOException {
        buffer.put((byte) b);
        if (!buffer.hasRemaining()) {
            logger.info("remaining buffer size: {} for blob name: {}", buffer.remaining(), blobName);
            uploadPart();
        }
        position++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {

        // Sanity check
        if (b == null) {
            throw new NullPointerException("Cannot write empty byte array");

        } else if (isOutOfRange(off, b.length) || len < 0 || isOutOfRange(off + len, b.length)) {
            throw new IndexOutOfBoundsException("Out of range values");

        } else if (len == 0) {
            return;
        }

        if (buffer.remaining() <= len) {
            logger.info("remaining buffer size: {} for blob name: {}", buffer.remaining(), blobName);
            int firstPart = buffer.remaining();
            buffer.put(b, off, firstPart);
            position += firstPart;
            uploadPart();
            write(b, off + firstPart, len - firstPart);

        } else {
            buffer.put(b, off, len);
            position += len;
        }
    }

    @Override
    public void close() throws IOException {
        logger.info("ParquetOutputStream: close initiated");
        if (commit) {
            commit();
            commit = false;
        } else {
            internalClose();
        }
    }

    private void commit() throws IOException {
        if (closed) {
            logger.warn("Tried to commit data for blob: {} on a closed stream", blobName);
            return;
        }
        try {
            logger.info("ParquetOutputStream: commit initiated");
            if (buffer.hasRemaining()) {
                uploadPart(buffer.position());
                logger.info("Data upload complete for blob: {}", blobName);
            }
        } catch (Exception e) {
            logger.warn("Data upload failed for blob: {} with exception: {}", blobName, e.getMessage());
            throw e;

        } finally {
            buffer.clear();
            internalClose();
        }
    }

    private void internalClose() throws IOException {
        logger.info("ParquetOutputStream: internal close initiated");
        if (closed) {
            return;
        }
        closed = true;
        super.close();
    }

    private static boolean isOutOfRange(int off, int len) {
        return off < 0 || off > len;
    }

    private void uploadPart() throws IOException {
        uploadPart(this.partSize);
        buffer.clear();
    }

    private void uploadPart(final int partSize) throws IOException {
        logger.info("uploading part for blob name: {}", blobName);
        try {
            byte[] slicedBuf = Arrays.copyOfRange(buffer.array(), 0, partSize);
            this.storageManager.append(this.blobName, slicedBuf);
        } catch (Exception e) {
            throw new IOException("Part upload failed", e);
        }
    }
}
