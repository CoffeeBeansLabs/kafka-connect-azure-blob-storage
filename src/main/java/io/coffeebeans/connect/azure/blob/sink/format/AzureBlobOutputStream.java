package io.coffeebeans.connect.azure.blob.sink.format;

import io.coffeebeans.connect.azure.blob.sink.format.bytearray.ByteArrayRecordWriter;
import io.coffeebeans.connect.azure.blob.sink.format.json.JsonRecordWriter;
import io.coffeebeans.connect.azure.blob.sink.storage.StorageManager;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This output stream maintains a buffer of data.
 * When the buffer is full it uploads the data to
 * the Azure Blob Storage service.
 */
public class AzureBlobOutputStream extends PositionOutputStream {
    private static final Logger log = LoggerFactory.getLogger(AzureBlobOutputStream.class);

    private long position;
    private boolean isClosed;
    private int compressionLevel;
    private boolean shouldThrowException;
    private OutputStream compressionFilter;
    private CompressionType compressionType;

    private final int blockSize;
    private final String blobName;
    private final ByteBuffer buffer;
    private final List<String> base64BlockIds;
    private final StorageManager storageManager;
    private final Base64.Encoder base64Encoder;

    /**
     * Construct a {@link AzureBlobOutputStream}.
     *
     * @param storageManager Storage manager to interact with blob storage
     * @param blobName Name of the blob where data will be stored
     * @param blockSize Size of the buffer
     */
    public AzureBlobOutputStream(StorageManager storageManager, String blobName, int blockSize) {
        this.position = 0L;
        this.isClosed = false;
        this.compressionLevel = -1;
        this.shouldThrowException = false;

        this.blockSize = blockSize;
        this.blobName = blobName;
        this.storageManager = storageManager;

        this.buffer = ByteBuffer.allocate(blockSize);
        this.base64BlockIds = new LinkedList<>();
        this.base64Encoder = Base64.getEncoder();

        log.debug("Configured output stream with part size: {}, for blob: {}", blockSize, blobName);
    }

    @Override
    public long getPos() {
        return position;
    }

    @Override
    public void write(int b) throws IOException {
        checkIfExceptionHasToBeThrown();

        buffer.put((byte) b);
        if (!buffer.hasRemaining()) {
            log.debug("remaining buffer size: {} for blob: {}", buffer.remaining(), blobName);
            stageBlock();
        }
        position++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkIfExceptionHasToBeThrown();

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
            stageBlock();

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

    /**
     * Internal close.
     *
     * @throws IOException thrown if encounters any error while closing the stream.
     */
    public void internalClose() throws IOException {
        if (isClosed) {
            return;
        }
        isClosed = true;
        super.close();
    }

    /**
     * Sends all the data to the output file and commits it.
     *
     * @throws IOException thrown if encounters any error while committing data
     */
    public void commit() throws IOException {
        if (isClosed) {
            log.warn("Commit operation invoked but the stream was closed, blob: {}", blobName);
            return;
        }
        try {
            log.debug("Commit operation invoked for blob: {}", blobName);
            if (compressionType != null) {
                compressionType.finalize(compressionFilter);
            }
            if (buffer.hasRemaining()) {
                stageBlock(buffer.position(), true);
            }
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
     * Wrap this output stream for compression. Used by
     * {@link JsonRecordWriter JsonRecordWriter} and
     * {@link ByteArrayRecordWriter ByteArrayRecordWriter}
     * as they do not support compression out of the box.
     *
     * @return Wrapped output stream
     */
    public OutputStream wrapForCompression() {
        if (compressionFilter == null) {
            compressionFilter = compressionType.wrapForOutput(this, compressionLevel);
        }
        return compressionFilter;
    }

    /**
     * Stages the block.
     */
    private void stageBlock() throws IOException {
        stageBlock(this.blockSize, false);

        /*
        Clearing the buffer does not erase the existing data in the buffer
        It just reset the pointer location to first index and overwrite the
        existing byte from there.
         */
        buffer.clear();
    }

    private void stageBlock(final int partSize, boolean shouldCommit) {
        try {
            /*
            Adding block id to the list before the staging operation is complete
            because we have to preserve the order, or it will create
            corrupt or incorrect files.
             */
            String blockId = generateBase64RandomBlockId();
            this.base64BlockIds.add(blockId);

            log.debug("Initiated staging block of id: {} for blob: {}", blockId, blobName);

            byte[] slicedBuf = Arrays.copyOfRange(buffer.array(), 0, partSize);

            this.storageManager.stageBlockAsync(blobName, blockId, slicedBuf)
                    .subscribe(
                            success -> log.debug("Staging for block id: {} on blob: {} was successful",
                                    blockId, blobName),
                            error -> {
                                /*
                                Error is raised only after timeout and
                                exhausting all the retries. If still it encounters
                                error, there is something really wrong with the
                                external system.

                                Setting this flag to true so that,
                                Exception can be thrown on the main thread.
                                 */
                                this.shouldThrowException = true;
                            });

            /*
            Commit will be invoked when closing the file.
             */
            if (!shouldCommit) {
                return;
            }

            this.storageManager.commitBlockIdsAsync(blobName, base64BlockIds, false)
                    .subscribe(
                            success -> log.info("Commit successful for blob: {}", blobName),
                            error -> log.error("Commit failed for blob: {}", blobName));

        } catch (Exception e) {
            throw new RetriableException("Failed staging for blob: " + blobName, e);
        }
    }

    private String generateBase64RandomBlockId() {
        return base64Encoder.encodeToString(
                UUID.randomUUID()
                        .toString()
                        .getBytes(StandardCharsets.UTF_8)
        );
    }

    private void checkIfExceptionHasToBeThrown() throws RetriableException {
        if (!shouldThrowException) {
            return;
        }
        throw new RetriableException("Failed staging one of the block for blob: {}" + blobName);
    }

    public AzureBlobOutputStream setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    public AzureBlobOutputStream setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;
        return this;
    }
}
