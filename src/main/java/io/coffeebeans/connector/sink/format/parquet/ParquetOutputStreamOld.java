package io.coffeebeans.connector.sink.format.parquet;

import io.coffeebeans.connector.sink.storage.BlobStorageManager;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ParquetOutputStreamOld extends PositionOutputStream {
    private static final Logger logger = LoggerFactory.getLogger(ParquetOutputStreamOld.class);

    private long position;
    private final ByteBuffer buffer;

    public ParquetOutputStreamOld() {
        this.position = 0;
        this.buffer = ByteBuffer.allocate(1024 * 1024);
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public void write(int b) throws IOException {
        logger.info("Writing single byte to output stream, {}", b);
        buffer.put((byte) b);
        position++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        logger.info("Writing bytes to output stream with len {}", len);
        buffer.put(b, off, len);
        position += len;
    }

    public byte[] toByteArray() {
        byte[] bytes = buffer.array();
        buffer.clear();
        return bytes;
    }
}
