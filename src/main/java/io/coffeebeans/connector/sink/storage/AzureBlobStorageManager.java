package io.coffeebeans.connector.sink.storage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlobClientBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

public class AzureBlobStorageManager {
    private static final Logger logger = LoggerFactory.getLogger(AzureBlobStorageManager.class);

    private final BlobServiceClient serviceClient;

    public AzureBlobStorageManager(String connectionUrl) {
        // Init service client
        this.serviceClient = new BlobServiceClientBuilder()
                .connectionString(connectionUrl)
                .buildClient();
    }

    public void upload(String containerName, String blobName, byte[] data) {
        BlobContainerClient containerClient = this.serviceClient
                .getBlobContainerClient(containerName);

        AppendBlobClient appendBlobClient = containerClient.getBlobClient(blobName).getAppendBlobClient();
        createAppendBlobIfNotExist(appendBlobClient);

        appendBlobClient.appendBlock(new ByteArrayInputStream(data), data.length);
    }

    private void createAppendBlobIfNotExist(AppendBlobClient appendBlobClient) {
        if (!appendBlobClient.exists()) {
            appendBlobClient.create();
        }
    }
}
