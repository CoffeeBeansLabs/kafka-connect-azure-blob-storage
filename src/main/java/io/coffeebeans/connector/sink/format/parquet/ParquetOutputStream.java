package io.coffeebeans.connector.sink.format.parquet;

import io.coffeebeans.connector.sink.storage.Storage;
import io.coffeebeans.connector.sink.storage.StorageFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used by ParquetWriter to flush the data from in-memory store to some sink
 * (e.g. Files). This is an implementation of PositionOutputStream which will
 * maintain a buffer. When the buffer is full it will upload the data to
 * the Azure Blob Storage service.
 * <br><br>
 * As of now it performs APPEND operation on a blob.
 */
public class ParquetOutputStream extends PositionOutputStream {
    private static final Logger log = LoggerFactory.getLogger(ParquetOutputStream.class);

    private long position;
    private boolean closed;
    private boolean commit;
    private final int partSize;
    private final String blobName;
    private final ByteBuffer buffer;
    private final Storage azureStorage;

    /**
     * Pass the blob name with complete path and the part size as arguments.
     * The blob name should be complete (prefixed with the directory information
     * and suffixed with extension). The part size is the size of the buffer.
     *
     * @param blobName Name of the blob (prefixed with the directory
     *                 information and suffixed with extension)
     * @param partSize Part size to be used to allocated size to byte buffer
     */
    public ParquetOutputStream(String blobName, int partSize) {
        this.position = 0L;
        this.closed = false;
        this.commit = false;
        this.blobName = blobName;
        this.partSize = partSize;
        this.buffer = ByteBuffer.allocate(this.partSize);
        this.azureStorage = StorageFactory.get();

        log.info("Configured parquet output stream with part size: {}, for blob: {}", this.partSize, blobName);
    }

    /**
     * Set the commit flag. The commit flag is checked while
     * {@link #close() close()} method is invoked.
     *
     * @param commit boolean value to be set to commit flag
     */
    public void setCommit(boolean commit) {
        this.commit = commit;
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    /**
     * Writes the specified byte to this output stream. The general contract for
     * <code>write</code> is that one byte is written to the output stream.
     * The byte to be written is the eight low-order bits of the argument
     * <code>b</code>. The 24 high-order bits of <code>b</code> are ignored.
     * <br><br>
     * The specified byte is put into the byte buffer. If the buffer is full
     * after writing byte into it, all the data from buffer is appended to blob in
     * Azure Blob Storge service and the buffer is cleared.
     *
     * @param      b   the <code>byte</code>.
     * @exception  IOException  if an I/O error occurs. In particular, an
     *             <code>IOException</code> may be thrown if the
     *             output stream has been closed / problem with azure blob
     *             storage service.
     */
    @Override
    public void write(int b) throws IOException {
        buffer.put((byte) b);
        if (!buffer.hasRemaining()) {
            log.debug("remaining buffer size: {} for blob: {}", buffer.remaining(), blobName);
            uploadPart();
        }
        position++;
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to this output stream.
     * The general contract for <code>write(b, off, len)</code> is that
     * some bytes in the array <code>b</code> are written to the
     * output stream in order; element <code>b[off]</code> is the first
     * byte written and <code>b[off+len-1]</code> is the last byte written
     * by this operation.
     *
     * <p>The implementation first confirms if the byte buffer's remaining
     * space is less than the length. If it is true then the data has to
     * be processed in multiple parts (recursively).
     *
     * <p>If <code>b</code> is <code>null</code>, a
     * <code>NullPointerException</code> is thrown.
     *
     * <p>If <code>off</code> is negative, or <code>len</code> is negative, or
     * <code>off+len</code> is greater than the length of the array
     * {@code b}, then an {@code IndexOutOfBoundsException} is thrown.
     *
     * @param      b     the byte array (data).
     * @param      off   the start offset in the data.
     * @param      len   the number of bytes to write.
     * @exception  IOException  if an I/O error occurs. In particular,
     *             an <code>IOException</code> is thrown if the output
     *             stream is closed.
     */
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

    /**
     * Closes this output stream and releases any system resources
     * associated with this stream. The general contract of <code>close</code>
     * is that it closes the output stream. A closed stream cannot perform
     * output operations and cannot be reopened.
     *
     * <p>This implementation will invoke the {@link #commit() commit()} method to
     * perform the commit and then clear and close any resource being used.
     *
     * @exception  IOException  if an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        log.info("Close operation invoked for blob: {}", blobName);
        if (commit) {
            commit();
            commit = false;
        } else {
            internalClose();
        }
    }

    /**
     * Before clearing / closing all the resources, the remaining
     * data has to be uploaded to the Azure Blob Storage service.
     * If the output stream is not yet closed, it will check if there
     * is any data still remaining in the buffer, if true then it will be
     * uploaded.
     *
     * @throws IOException if any I/O error occurs.
     */
    private void commit() throws IOException {
        if (closed) {
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

    /**
     * It just set the closed flag to true and call the super close method.
     *
     * @throws IOException If any I/O error occurs
     */
    private void internalClose() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        super.close();
    }

    private static boolean isOutOfRange(int off, int len) {
        return off < 0 || off > len;
    }

    /**
     * Call the {@link #uploadPart(int)} method and once upload is done, clear the buffer.
     *
     * @throws IOException If any I/O error occurs
     */
    private void uploadPart() throws IOException {
        uploadPart(this.partSize);

        /*
        Clearing the buffer does not erase the existing data in the buffer
        It just reset the pointer location to first index and overwrite the
        existing byte from there.
         */
        buffer.clear();
    }

    /**
     * It will the upload the data up to the partSize from
     * the byte buffer. It interacts with the Storage service
     * to append the data to the blob.
     *
     * @param partSize how much data to be taken from buffer for uploading
     * @throws IOException If any I/O error occurs
     */
    private void uploadPart(final int partSize) throws IOException {
        log.info("uploading part for blob name: {}", blobName);
        try {
            byte[] slicedBuf = Arrays.copyOfRange(buffer.array(), 0, partSize);
            this.azureStorage.append(this.blobName, slicedBuf);

        } catch (Exception e) {
            log.error("Failed to upload part data to blob: {}, with exception: {}", blobName, e.getMessage());
            throw new IOException("Failed to upload part data to blob: " + blobName, e);
        }
    }
}
