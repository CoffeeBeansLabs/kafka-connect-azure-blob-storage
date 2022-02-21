package io.coffeebeans.connector.sink.storage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.AppendBlobClient;

import java.io.ByteArrayInputStream;

public class AzureBlobStorageManager {
    private final BlobServiceClient serviceClient;

    public AzureBlobStorageManager(String connectionString) {

        this.serviceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
    }

    public void upload(String containerName, String blobName, byte[] data) {
        BlobContainerClient containerClient = this.serviceClient
                .getBlobContainerClient(containerName);

        AppendBlobClient appendBlobClient = containerClient.getBlobClient(blobName).getAppendBlobClient();

        if (!appendBlobClient.exists()) {
            appendBlobClient.create();
        }
        appendBlobClient.appendBlock(new ByteArrayInputStream(data), data.length);
    }
}
