package io.coffeebeans.connector.sink.format.parquet;

import java.io.IOException;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * This class will be used by FormatWriter. And it's object will be passed to ParquetWriter which will internally call
 * it to create and write data to the output stream.
 */
public class ParquetOutputFileOld implements OutputFile {

    private ParquetOutputStreamOld parquetOutputStreamOld;

    /**
     * Constructor.
     *
     * @param byteArrayOutputStream ByteArrayOutputStream
     */
    public ParquetOutputFileOld() {
    }

    @Override
    public PositionOutputStream create(long l) throws IOException {
        return createPositionOutputStream();
    }

    private PositionOutputStream createPositionOutputStream() {
        this.parquetOutputStreamOld = new ParquetOutputStreamOld();
        return this.parquetOutputStreamOld;
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
        return this.parquetOutputStreamOld.toByteArray();
    }
}
