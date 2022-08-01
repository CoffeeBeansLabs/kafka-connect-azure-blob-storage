package io.coffeebeans.connector.sink;

import io.coffeebeans.connector.sink.format.FileFormat;
import io.coffeebeans.connector.sink.format.RecordWriterProvider;
import io.coffeebeans.connector.sink.format.SchemaStore;
import io.coffeebeans.connector.sink.format.avro.AvroSchemaStore;
import io.coffeebeans.connector.sink.format.parquet.ParquetRecordWriterProvider;
import io.coffeebeans.connector.sink.storage.AzureBlobStorageManager;
import io.coffeebeans.connector.sink.storage.StorageManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for AzureBlobSinkTask.
 */
public class AzureBlobSinkTaskTest {
    private static final String CONNECTION_URL_VALID = "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K"
            + "1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://"
            + "host.docker.internal:10000/devstoreaccount1;";

    //    @Test
    //    void getPartitioner_givenTimePartitionStrategy_shouldReturnTimePartitioner() {
    //    }

    @Test
    @DisplayName("Given Parquet file format, getRecordWriterProvider should return ParquetRecordWriter instance")
    void getRecordWriterProvider_givenParquetFileFormat_shouldReturnParquetRecordWriter() {

        String fileFormat = FileFormat.PARQUET.toString();
        AzureBlobSinkTask azureBlobSinkTask = new AzureBlobSinkTask();

        RecordWriterProvider recordWriterProvider = azureBlobSinkTask.getRecordWriterProvider(fileFormat);

        Assertions.assertTrue(recordWriterProvider instanceof ParquetRecordWriterProvider);
    }

    @Test
    @DisplayName("Given Parquet file format, getSchemaStore should return AvroSchemaStore instance")
    void getSchemaStore_givenParquetFileFormat_shouldReturnAvroSchemaStore() {

        String fileFormat = FileFormat.PARQUET.toString();
        AzureBlobSinkTask azureBlobSinkTask = new AzureBlobSinkTask();

        SchemaStore schemaStore = azureBlobSinkTask.getSchemaStore(fileFormat);

        Assertions.assertTrue(schemaStore instanceof AvroSchemaStore);
    }

    @Test
    @DisplayName("Given connection URL and container name, getStorage should return AzureBlobStorage")
    void getStorage_givenConnectionUrlAndContainerName_shouldReturnAzureBlobStorage() {

        String containerName = "test-container";
        AzureBlobSinkTask azureBlobSinkTask = new AzureBlobSinkTask();

        StorageManager storageManager = azureBlobSinkTask.getStorage(CONNECTION_URL_VALID, containerName);

        Assertions.assertTrue(storageManager instanceof AzureBlobStorageManager);
    }
}
