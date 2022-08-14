package io.coffeebeans.connector.sink.format;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Supported compression types for JSON and Binary format.
 */
public enum CompressionType {

    NONE("none", ""),
    GZIP("gzip", ".gz") {

        @Override
        public OutputStream wrapForOutput(OutputStream out) {
            return wrapForOutput(out, Deflater.DEFAULT_COMPRESSION);
        }

        @Override
        public OutputStream wrapForOutput(OutputStream out, int level) {
            try {
                return new GZIPOutputStream(out, GZIP_BUFFER_SIZE_BYTES) {
                    public OutputStream setLevel(int level) {
                        def.setLevel(level);
                        return this;
                    }
                }.setLevel(level);
            } catch (Exception e) {
                throw new ConnectException(e);
            }
        }

        @Override
        public InputStream wrapForInput(InputStream in) {
            try {
                return new GZIPInputStream(in);
            } catch (Exception e) {
                throw new ConnectException(e);
            }
        }

        @Override
        public void finalize(OutputStream compressionFilter) {
            if (compressionFilter instanceof DeflaterOutputStream) {
                try {
                    ((DeflaterOutputStream) compressionFilter).finish();
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
                return;
            }
            throw new ConnectException("Compression filter is not an instance of DeflaterOutputStream");
        }
    };

    private static final int GZIP_BUFFER_SIZE_BYTES = 8 * 1024;

    public final String name;
    public final String extension;

    CompressionType(String name, String extension) {
        this.name = name;
        this.extension = extension;
    }

    /**
     * Returns {@link CompressionType} for given name.
     *
     * @param name compression type
     * @return CompressionType
     */
    public static CompressionType forName(String name) {
        if (NONE.name.equalsIgnoreCase(name)) {
            return NONE;

        } else if (GZIP.name.equalsIgnoreCase(name)) {
            return GZIP;

        } else {
            throw new IllegalArgumentException("Unknown compression name: " + name);
        }
    }

    /**
     * Wrap {@code out} with a filter that will compress data with this CompressionType.
     *
     * @param out the {@link OutputStream} to wrap
     * @return a wrapped version of {@code out} that will apply compression
     */
    public OutputStream wrapForOutput(OutputStream out) {
        return out;
    }

    /**
     * Wrap {@code out} with a filter that will compress data with this CompressionType at the
     * given compression level (optional operation).
     *
     * @param out the {@link OutputStream} to wrap
     * @param level the compression level for this compression type
     * @return a wrapped version of {@code out} that will apply compression at the given level
     */
    public OutputStream wrapForOutput(OutputStream out, int level) {
        return wrapForOutput(out);
    }

    /**
     * Wrap {@code in} with a filter that will decompress data with this CompressionType.
     *
     * @param in the {@link InputStream} to wrap
     * @return a wrapped version of {@code in} that will apply decompression
     */
    public InputStream wrapForInput(InputStream in) {
        return in;
    }

    /**
     * Take any action necessary to finalize filter before the underlying
     * S3OutputStream is committed.
     *
     * <p>Implementations of this method should make sure to handle the case
     * where {@code compressionFilter} is null.
     *
     * @param compressionFilter a wrapped {@link OutputStream}
     */
    public void finalize(OutputStream compressionFilter) {}
}
