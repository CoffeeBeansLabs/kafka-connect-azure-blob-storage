package io.coffeebeans.connector.sink.storage;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import java.util.UUID;

public class AzureBlobStorageManager {
    private final BlobServiceClient serviceClient;

    public AzureBlobStorageManager(String connectionString) {

        this.serviceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
    }

    public void upload(String containerName, Object data) {
        BlobContainerClient containerClient = this.serviceClient
                .getBlobContainerClient(containerName);

        String blobName = UUID.randomUUID().toString();
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        blobClient.upload(BinaryData.fromObject(data));
    }
}
