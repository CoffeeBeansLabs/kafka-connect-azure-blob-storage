package io.coffeebeans.connect.azure.blob.sink.format.json;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonGenerator;
import io.coffeebeans.connect.azure.blob.sink.format.AzureBlobOutputStream;
import io.coffeebeans.connect.azure.blob.sink.format.CompressionType;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
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
 * Unit tests for {@link JsonRecordWriter}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class JsonRecordWriterTest {

    @Mock
    private SinkRecord sinkRecord;

    @Mock
    private AzureBlobOutputStream outputStream;

    @Mock
    private OutputStream wrapperOutputStream;

    @Mock
    private JsonConverter jsonConverter;

    @Mock
    private JsonGenerator jsonGenerator;

    @Mock
    private Struct struct;

    private JsonRecordWriter writer;

    /**
     * Init.
     */
    @BeforeEach
    public void init() throws NoSuchFieldException, IllegalAccessException, IOException {

        writer = new JsonRecordWriter(
                null,
                CompressionType.NONE,
                -1,
                10000,
                "test-blob",
                100
        );

        // Injecting Mocked JsonConverter
        Field jsonConverterField = writer
                .getClass().getDeclaredField("jsonConverter");

        jsonConverterField.setAccessible(true);
        jsonConverterField.set(writer, jsonConverter);

        // Injecting Mocked JsonGenerator
        Field jsonGeneratorField = writer
                .getClass().getDeclaredField("jsonGenerator");

        jsonGeneratorField.setAccessible(true);
        jsonGeneratorField.set(writer, jsonGenerator);

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
     * <b>Method: {@link JsonRecordWriter#write(SinkRecord)}</b>.<br>
     */
    @Test
    @DisplayName("Given struct, write should write data to wrapper output stream")
    void write_givenStruct_shouldWriteDataToWrapperOutputStream() throws IOException {

        when(sinkRecord.value())
                .thenReturn(struct);
        when(jsonConverter.fromConnectData(any(), any(), any()))
                .thenReturn(null);

        writer.write(sinkRecord);
        verify(wrapperOutputStream, times(2))
                .write(any());
    }

    /**
     * <b>Method: {@link JsonRecordWriter#write(SinkRecord)}</b>.<br>
     */
    @Test
    @DisplayName("Given Map, write should write data to Json generator")
    void write_givenMap_shouldWriteDataToJsonGenerator() throws IOException {

        when(sinkRecord.value())
                .thenReturn(new HashMap<>());

        writer.write(sinkRecord);
        verify(jsonGenerator, times(1))
                .writeObject(any());
        verify(jsonGenerator, times(1))
                .writeRaw(anyString());
    }

    /**
     * <b>Method: {@link JsonRecordWriter#commit()}</b>.<br>
     * <b>Expectations: </b>
     * <ul>
     *     <li>Should invoke flush on json generator</li>
     *     <li>Should invoke commit on Output stream</li>
     *     <li>Should invoke close on wrapper output stream</li>
     * </ul>
     */
    @Test
    @DisplayName("Commit should invoke flush on json generator commit on output stream"
            + " and close on wrapper output stream")
    void commit_shouldInvokeFlushOnJsonGeneratorCommitOnOutputStreamAndCloseOnWrapperOutputStream() throws IOException {

        writer.commit();

        verify(jsonGenerator, times(1))
                .flush();
        verify(outputStream, times(1))
                .commit();
        verify(wrapperOutputStream, times(1))
                .close();
    }

    /**
     * <b>Method: {@link JsonRecordWriter#close()}</b>.<br>
     * <b>Expectations: </b>
     * <ul>
     *     <li>Should invoke close on json generator</li>
     * </ul>
     */
    @Test
    @DisplayName("Close should invoke close on Json generator")
    void close_shouldInvokeCloseOnJsonGenerator() throws IOException {

        writer.close();
        verify(jsonGenerator, times(1))
                .close();
    }
}
