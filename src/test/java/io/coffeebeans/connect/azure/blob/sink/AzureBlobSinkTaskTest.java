package io.coffeebeans.connect.azure.blob.sink;

import io.coffeebeans.connect.azure.blob.sink.format.Format;
import io.coffeebeans.connect.azure.blob.sink.format.RecordWriterProvider;
import io.coffeebeans.connect.azure.blob.sink.format.SchemaStore;
import io.coffeebeans.connect.azure.blob.sink.format.avro.AvroSchemaStore;
import io.coffeebeans.connect.azure.blob.sink.format.parquet.ParquetRecordWriterProvider;
import io.coffeebeans.connect.azure.blob.sink.storage.AzureBlobStorageManager;
import io.coffeebeans.connect.azure.blob.sink.storage.StorageManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AzureBlobSinkTask}.
 */
public class AzureBlobSinkTaskTest {
    private static final String CONNECTION_URL_VALID = "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K"
            + "1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://"
            + "host.docker.internal:10000/devstoreaccount1;";

    @Test
    @DisplayName("Given Parquet file format, getRecordWriterProvider should return ParquetRecordWriter instance")
    void getRecordWriterProvider_givenParquetFileFormat_shouldReturnParquetRecordWriter() {

        String fileFormat = Format.PARQUET.toString();
        AzureBlobSinkTask azureBlobSinkTask = new AzureBlobSinkTask();

        RecordWriterProvider recordWriterProvider = azureBlobSinkTask.getRecordWriterProvider(fileFormat);

        Assertions.assertTrue(recordWriterProvider instanceof ParquetRecordWriterProvider);
    }

    @Test
    @DisplayName("Given Parquet file format, getSchemaStore should return AvroSchemaStore instance")
    void getSchemaStore_givenParquetFileFormat_shouldReturnAvroSchemaStore() {

        String fileFormat = Format.PARQUET.toString();
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
