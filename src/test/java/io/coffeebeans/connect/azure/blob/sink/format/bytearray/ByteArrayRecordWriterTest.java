package io.coffeebeans.connect.azure.blob.sink.format.bytearray;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.coffeebeans.connect.azure.blob.sink.format.AzureBlobOutputStream;
import io.coffeebeans.connect.azure.blob.sink.format.CompressionType;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Unit tests for {@link ByteArrayRecordWriter}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ByteArrayRecordWriterTest {

    @Mock
    private ByteArrayConverter byteArrayConverter;

    @Mock
    private AzureBlobOutputStream outputStream;

    @Mock
    private OutputStream wrapperOutputStream;

    @Mock
    private SinkRecord sinkRecord;

    private ByteArrayRecordWriter writer;

    /**
     * Init.
     */
    @BeforeEach
    public void init() throws NoSuchFieldException, IllegalAccessException {

        writer = new ByteArrayRecordWriter(
                null, CompressionType.NONE,
                -1, 5, "test", "kTopic"
        );

        // Injecting Mocked ByteArrayConverter
        Field byteArrayConverterField = writer
                .getClass().getDeclaredField("byteArrayConverter");

        byteArrayConverterField.setAccessible(true);
        byteArrayConverterField.set(writer, byteArrayConverter);

        // Injecting Mocked output stream
        Field outputStreamField = writer
                .getClass().getDeclaredField("outputStream");

        outputStreamField.setAccessible(true);
        outputStreamField.set(writer, outputStream);

        // Injecting Mocked wrapperOutputStream
        Field wrapperOutputStreamField = writer
                .getClass().getDeclaredField("outputStreamCompressionWrapper");

        wrapperOutputStreamField.setAccessible(true);
        wrapperOutputStreamField.set(writer, wrapperOutputStream);
    }

    /**
     * <b>Method: {@link ByteArrayRecordWriter#write(SinkRecord)}</b>.<br>
     */
    @Test
    @DisplayName("Given sink record, write should write data to wrapper output stream")
    void write_givenSinkRecord_shouldWriteDataToWrapperOutputStream() throws IOException {

        when(byteArrayConverter.fromConnectData(any(), any(), any()))
                .thenReturn(null);

        writer.write(sinkRecord);

        verify(wrapperOutputStream, times(2))
                .write(any());
    }

    /**
     * <b>Method: {@link ByteArrayRecordWriter#commit()}</b>.<br>
     * <b>Expectations: </b>
     * <ul>
     *     <li>Should invoke commit on Output stream</li>
     *     <li>Should invoke close on wrapper output stream</li>
     * </ul>
     */
    @Test
    @DisplayName("Commit should invoke commit on output stream and close on wrapper output stream")
    void commit_shouldInvokeCommitOnOutputStreamAndCloseOnWrapperOutputStream() throws IOException {

        writer.commit();

        verify(outputStream, times(1))
                .commit();
        verify(wrapperOutputStream, times(1))
                .close();
    }
}
