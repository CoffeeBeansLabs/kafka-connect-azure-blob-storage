package io.coffeebeans.connector.sink.format.parquet;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.RecordWriterProvider;
import io.coffeebeans.connector.sink.format.SchemaStore;
import io.coffeebeans.connector.sink.storage.StorageManager;
import io.confluent.connect.avro.AvroData;

/**
 * ParquetRecordWriterProvider is responsible for creating
 * and providing new instances of ParquetRecordWriter.
 */
public class ParquetRecordWriterProvider implements RecordWriterProvider {
    private static final String EXTENSION = ".parquet";

    private SchemaStore schemaStore;

    public ParquetRecordWriterProvider(SchemaStore schemaStore) {
        this.schemaStore = schemaStore;
    }

    /**
     * It will initialize the in lazy-loading fashion
     * if not already initialized. It will concatenate the file name with
     * the extension of Parquet file format and create a new instance of
     * ParquetRecordWriter.
     *
     * @param fileName Blob name (Prefixed with the directory info.)
     * @return Record Writer to write parquet files
     */
    public RecordWriter getRecordWriter(AzureBlobSinkConfig config,
                                        StorageManager storageManager,
                                        final String fileName,
                                        String topic) {

        String blobName = fileName + getExtension();
        int partSize = config.getPartSize();

        return new ParquetRecordWriter(
                storageManager,
                schemaStore,
                partSize,
                blobName,
                topic
        );
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