package io.coffeebeans.connector.sink.format.parquet;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * This class will be used by FormatWriter. And it's object will be passed to ParquetWriter which will internally call
 * it to create and write data to the output stream.
 */
public class ParquetOutputFile implements OutputFile {
    private final BufferedOutputStream bufferedOutputStream;
    private final ByteArrayOutputStream byteArrayOutputStream;

    /**
     * Constructor.
     *
     * @param byteArrayOutputStream ByteArrayOutputStream
     */
    public ParquetOutputFile(ByteArrayOutputStream byteArrayOutputStream) {
        this.byteArrayOutputStream = byteArrayOutputStream;
        this.bufferedOutputStream = new BufferedOutputStream(byteArrayOutputStream);
    }

    @Override
    public PositionOutputStream create(long l) throws IOException {
        return createPositionOutputStream();
    }

    private PositionOutputStream createPositionOutputStream() {
        return new PositionOutputStream() {

            int pos = 0;

            @Override
            public long getPos() {
                return pos;
            }

            @Override
            public void flush() throws IOException {
                bufferedOutputStream.flush();
            }

            @Override
            public void close() throws IOException {
                bufferedOutputStream.close();
            }

            @Override
            public void write(int b) throws IOException {
                bufferedOutputStream.write(b);
                pos++;
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                bufferedOutputStream.write(b, off, len);
                pos += len;
            }
        };
    }

    @Override
    public PositionOutputStream createOrOverwrite(long l) throws IOException {
        return createPositionOutputStream();
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    public byte[] toByteArray() {
        return this.byteArrayOutputStream.toByteArray();
    }
}
