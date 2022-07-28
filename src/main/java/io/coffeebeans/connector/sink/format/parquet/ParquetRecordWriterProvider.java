package io.coffeebeans.connector.sink.format.parquet;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.RecordWriterProvider;
import io.coffeebeans.connector.sink.format.SchemaStore;
import io.confluent.connect.avro.AvroData;

/**
 * ParquetRecordWriterProvider is responsible for creating
 * and providing new instances of ParquetRecordWriter.
 */
public class ParquetRecordWriterProvider implements RecordWriterProvider {
    private static final String EXTENSION = ".parquet";

    private AvroData avroData;
    private SchemaStore schemaStore;

    public ParquetRecordWriterProvider(SchemaStore schemaStore) {
        this.schemaStore = schemaStore;
    }

    /**
     * It will initialize the {@link #avroData} in lazy-loading fashion
     * if not already initialized. It will concatenate the file name with
     * the extension of Parquet file format and create a new instance of
     * ParquetRecordWriter.
     *
     * @param config Config object of the connector
     * @param fileName Blob name (Prefixed with the directory info.)
     * @return Record Writer to write parquet files
     */
    public RecordWriter getRecordWriter(final AzureBlobSinkConfig config, final String fileName, String topic) {
        if (avroData == null) {
            avroData = new AvroData(20); // Lazy initialization
        }
        String blobName = fileName + getExtension();
        return new ParquetRecordWriter(config, schemaStore, avroData, blobName, topic);
    }

    /**
     * Get the extension of Parquet file.
     *
     * @return Parquet extension
     */
    public String getExtension() {
        return EXTENSION;
    }
}