package io.coffeebeans.connector.sink.format;

import io.coffeebeans.connector.sink.storage.StorageManager;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzureBlobOutputStream extends PositionOutputStream {
    private final Logger log = LoggerFactory.getLogger(AzureBlobOutputStream.class);

    private long position;
    private boolean isClosed;

    private final int partSize;
    private final String blobName;
    private final ByteBuffer buffer;
    private final StorageManager storageManager;

    public AzureBlobOutputStream(StorageManager storageManager, String blobName, int partSize) {
        this.position = 0L;
        this.isClosed = false;

        this.partSize = partSize;
        this.blobName = blobName;
        this.storageManager = storageManager;

        this.buffer = ByteBuffer.allocate(partSize);

        log.info("Configured output stream with part size: {}, for blob: {}", partSize, blobName);
    }

    @Override
    public long getPos() {
        return position;
    }

    @Override
    public void write(int b) throws IOException {
        buffer.put((byte) b);
        if (!buffer.hasRemaining()) {
            log.debug("remaining buffer size: {} for blob: {}", buffer.remaining(), blobName);
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
            log.debug("remaining buffer size: {}, length of data: {}, for blob: {}", buffer.remaining(), len, blobName);

            /*
            For e.g. If the size of the buffer is 100, remaining size of
            buffer is 15 and the length of the incoming byte array is 20,
            putting all the data into the buffer will result into buffer
            overflow exception. So we need to divide (logically and not by
            creating another array) the incoming byte array into two parts and
            put the first 15 bytes into the buffer and invoke uploadPart
            and then process the remaining 5 bytes of data
             */

            // Processing the first part of data
            int firstPart = buffer.remaining();
            buffer.put(b, off, firstPart);
            position += firstPart;

            // Uploading data
            uploadPart();

            // Processing the second part of data. It's a recursive operation, so it will handle large amount of data
            write(b, off + firstPart, len - firstPart);

        } else {
            buffer.put(b, off, len);
            position += len;
        }
    }

    private static boolean isOutOfRange(int off, int len) {
        return off < 0 || off > len;
    }

    @Override
    public void close() throws IOException {
        internalClose();
    }

    public void internalClose() throws IOException {
        if (isClosed) {
            return;
        }
        isClosed = true;
        super.close();
    }

    public void commit() throws IOException {
        if (isClosed) {
            log.warn("Commit operation invoked but the stream was closed, blob: {}", blobName);
            return;
        }
        try {
            log.info("Commit operation invoked for blob: {}", blobName);
            if (buffer.hasRemaining()) {
                uploadPart(buffer.position());
                log.info("Data upload complete for blob: {}", blobName);
            }
        } catch (Exception e) {
            log.warn("Data upload failed for blob: {} with exception: {}", blobName, e.getMessage());
            throw e;

        } finally {
            /*
            Clearing the buffer does not erase the existing data in the buffer
            It just reset the pointer location to first index and overwrite the
            existing byte from there.
             */
            buffer.clear();
            internalClose();
        }
    }

    private void uploadPart() throws IOException {
        uploadPart(this.partSize);

        /*
        Clearing the buffer does not erase the existing data in the buffer
        It just reset the pointer location to first index and overwrite the
        existing byte from there.
         */
        buffer.clear();
    }

    private void uploadPart(final int partSize) throws IOException {
        log.info("uploading part for blob name: {}", blobName);
        try {
            byte[] slicedBuf = Arrays.copyOfRange(buffer.array(), 0, partSize);
            this.storageManager.append(this.blobName, slicedBuf);

        } catch (Exception e) {
            log.error("Failed to upload part data to blob: {}, with exception: {}", blobName, e.getMessage());
            throw new IOException("Failed to upload part data to blob: " + blobName, e);
        }
    }
}
