package io.coffeebeans.connect.azure.blob.sink.format.avro;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connect.azure.blob.sink.format.AzureBlobOutputStream;
import io.coffeebeans.connect.azure.blob.sink.format.SchemaStore;
import io.coffeebeans.connect.azure.blob.sink.storage.StorageManager;
import io.confluent.connect.avro.AvroData;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

/**
 * Unit tests for {@link AvroRecordWriter}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AvroRecordWriterTest {

    @Mock
    private DataFileWriter<Object> dataFileWriter;

    @Mock
    private AvroData avroData;

    @Mock
    private ObjectMapper mapper;

    @Mock
    private SchemaStore schemaStore;

    @Mock
    private StorageManager storageManager;

    @Mock
    private JsonAvroConverter jsonAvroConverter;

    @Mock
    private SinkRecord sinkRecord;

    @Mock
    private Struct struct;

    private AvroRecordWriter writer;

    /**
     * Mocking.
     */
    @BeforeEach
    public void init() throws Exception {

        writer = new AvroRecordWriter(
                storageManager,
                schemaStore,
                0,
                "test",
                "Ktopic",
                CodecFactory.nullCodec(),
                avroData);

        // Injecting mocked DataFileWriter.
        Field dataFileWriterField = writer
                .getClass()
                .getDeclaredField("dataFileWriter");

        dataFileWriterField.setAccessible(true);
        dataFileWriterField.set(writer, dataFileWriter);


        // Injecting mocked object mapper.
        Field objectMapperField = writer
                .getClass()
                .getDeclaredField("mapper");

        objectMapperField.setAccessible(true);
        objectMapperField.set(writer, mapper);

        // Injecting mocked JsonAvroConverter
        Field jsonAvroConverterField = writer
                .getClass()
                .getDeclaredField("jsonAvroConverter");

        jsonAvroConverterField.setAccessible(true);
        jsonAvroConverterField.set(writer, jsonAvroConverter);


        // Mocking function calls
        when(sinkRecord.valueSchema())
                .thenReturn(null);

        when(avroData.fromConnectSchema(any()))
                .thenReturn(null);

        when(avroData.fromConnectData(any(), any()))
                .thenReturn(new Object());
    }

    /**
     * <b>Method: {@link AvroRecordWriter#write(SinkRecord)}</b>.<br>
     * <b>Assumption: </b>
     * <ul>
     *     <li>Value is a {@link org.apache.kafka.connect.data.Struct Struct}</li>
     * </ul>
     *
     * <p><b>Expectation: </b>
     * <ul>
     *     <li>Should write to data file writer</li>
     * </ul>
     */
    @Test
    @DisplayName("Given struct, should write to data file writer")
    void write_givenStruct_shouldWriteToDataFileWriter() throws IOException {

        when(sinkRecord.value())
                .thenReturn(struct);
        writer.write(sinkRecord);

        verify(dataFileWriter, times(1))
                .append(any());
    }

    /**
     * <b>Method: {@link AvroRecordWriter#close()}</b>.<br>
     * <b>Expectation: </b>
     * <ul>
     *     <li>Should invoke {@link DataFileWriter#close()}</li>
     * </ul>
     */
    @Test
    @DisplayName("Close should invoke data file writer close")
    void close_shouldInvokeDataFileWriterClose() throws IOException {

        writer.close();
        verify(dataFileWriter, times(1))
                .close();
    }

    /**
     * <b>Method: {@link AvroRecordWriter#commit()}</b>.<br>
     * <b>Expectation: </b>
     * <ul>
     *     <li>Should invoke {@link DataFileWriter#flush()}</li>
     *     <li>Should invoke {@link AzureBlobOutputStream#commit()}</li>
     *     <li>Should invoke {@link DataFileWriter#close()}</li>
     * </ul>
     */
    @Test
    @DisplayName("Commit should flush and close output stream and data file writer")
    void commit_shouldFlushAndCloseOutputStreamAndDataFileWriter() throws IOException {

        when(sinkRecord.value())
                .thenReturn(struct);
        writer.write(sinkRecord);
        writer.commit();

        verify(dataFileWriter, times(1))
                .flush();

        verify(dataFileWriter, times(1))
                .close();
    }

    /**
     * <b>Method: {@link AvroRecordWriter#write(SinkRecord)}</b>.<br>
     * <b>Assumption: </b>
     * <ul>
     *     <li>Value is a {@link String} (JSON-string)</li>
     * </ul>
     *
     * <p><b>Expectation: </b>
     * <ul>
     *     <li>Should write to data file writer</li>
     * </ul>
     */
    @Test
    @DisplayName("Given string, should write to data file writer")
    void write_givenString_shouldWriteToDataFileWriter() throws IOException {

        when(sinkRecord.value())
                .thenReturn("json-string");

        when(schemaStore.getSchema(anyString()))
                .thenReturn(null);

        when(jsonAvroConverter.convertToGenericDataRecord(any(), any()))
                .thenReturn(null);

        writer.write(sinkRecord);
        verify(dataFileWriter, times(1))
                .append(any());
    }

    /**
     * <b>Method: {@link AvroRecordWriter#write(SinkRecord)}</b>.<br>
     * <b>Assumption: </b>
     * <ul>
     *     <li>Value is a {@link java.util.Map Map}</li>
     * </ul>
     *
     * <p><b>Expectation: </b>
     * <ul>
     *     <li>Should write to data file writer</li>
     * </ul>
     */
    @Test
    @DisplayName("Given map, should write to data file writer")
    void write_givenMap_shouldWriteToDataFileWriter() throws IOException {

        when(sinkRecord.value())
                .thenReturn(new HashMap<>());

        when(mapper.writeValueAsString(any()))
                .thenReturn("test");

        when(schemaStore.getSchema(anyString()))
                .thenReturn(null);

        when(jsonAvroConverter.convertToGenericDataRecord(any(), any()))
                .thenReturn(null);

        writer.write(sinkRecord);
        verify(dataFileWriter, times(1))
                .append(any());
    }
}
