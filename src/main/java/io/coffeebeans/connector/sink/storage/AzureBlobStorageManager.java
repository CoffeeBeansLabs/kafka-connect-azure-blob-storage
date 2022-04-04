package io.coffeebeans.connector.sink.storage;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.AppendBlobItem;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.AppendBlobCreateOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.metadata.MetadataProducer;
import io.coffeebeans.connector.sink.model.Metadata;
import io.coffeebeans.connector.sink.partitioner.DefaultPartitioner;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will handle the implementation for uploading the data to blob storage service.
 */
public class AzureBlobStorageManager {
    private static final Logger logger = LoggerFactory.getLogger(AzureBlobStorageManager.class);
    private final BlobServiceClient serviceClient;
    private final ConcurrentMap<String, Integer> currentActiveIndex;

    private Long maxBlobSize;
    private MetadataProducer metadataProducer;

    /**
     * Constructor to be used for initializing the storage manager.
     *
     * @param connectionUrl Connection url provided by the user in the configuration.
     */
    public AzureBlobStorageManager(String connectionUrl) {
        // Init service client
        this.serviceClient = new BlobServiceClientBuilder()
                .connectionString(connectionUrl)
                .buildClient();

        this.currentActiveIndex = new ConcurrentHashMap<>();
        this.metadataProducer = new MetadataProducer();
    }

    public void configure(Map<String, String> configProps) {
        this.maxBlobSize = Long.parseLong(configProps.get(AzureBlobSinkConfig.ROLLOVER_POLICY_SIZE_CONF));
    }

    /**
     * I need the container name, blob name and the byte array of the data. I will upload the data to the container and
     * blob provided.
     *
     * @param containerName Container name
     * @param blobName Blob name
     * @param data Data as byte stream
     */
    public void upload(String containerName, String path, String blobName, byte[] data) throws JsonProcessingException,
            InterruptedException {

        // Get the container client
        BlobContainerClient containerClient = this.serviceClient
                .getBlobContainerClient(containerName);

        if (!currentActiveIndex.containsKey(path)) {

            // Create a metadata object
            int firstActiveIndex = 0;
            Metadata metadata = new Metadata(path, firstActiveIndex);

            // Create and append first metadata record
//            MetadataManager.appendMetadata(containerClient, path, metadata);
            metadataProducer.produceMetadata(metadata);

            // Update local current index
//            currentActiveIndex.put(path, firstActiveIndex);
            Thread.sleep(100);
            logger.info("metadata pushed: {}: {}", path, currentActiveIndex);
        }

        int currentActiveIndex = this.currentActiveIndex.get(path);

        // Create full blob path
        String blobFullPath = path + DefaultPartitioner.FOLDER_DELIMITER + blobName + DefaultPartitioner.FILE_DELIMITER
                + currentActiveIndex;

        // Get the AppendBlobClient
        AppendBlobClient appendBlobClient = containerClient.getBlobClient(blobFullPath).getAppendBlobClient();
        createAppendBlobIfNotExist(appendBlobClient);


        try {
            appendBlobClient.appendBlockWithResponse(
                    new ByteArrayInputStream(data), data.length, null,
                    new AppendBlobRequestConditions().setMaxSize(maxBlobSize), null, Context.NONE);

        } catch (BlobStorageException e) {

            if ((appendBlobClient.getProperties().getBlobSize() + data.length) > maxBlobSize) {
                logger.info("File size met, Rollover initiated");

                if (currentActiveIndex == this.currentActiveIndex.get(path)) {
                    currentActiveIndex++;
                    this.metadataProducer.produceMetadata(new Metadata(path, currentActiveIndex));
                    Thread.sleep(100);
                    logger.info("metadata pushed: {}: {}", path, currentActiveIndex);
                }

                upload(containerName, path, blobName, data);
//
//                try {
//                    // Retrieve metadata from blob storage service
//                    Metadata metadata = MetadataManager.retrieveMetadata(containerClient, path);
//
//                    int newActiveIndex = metadata.getIndex() + 1;
//
//                    // Append new metadata
//                    MetadataManager.appendMetadata(containerClient, path, new Metadata(path, newActiveIndex));
//
//                    // Update local index
//                    this.currentActiveIndex.put(path, newActiveIndex);
//
//                    // Retry appending data
//                    upload(containerName, path, blobName, data);
//
//                } catch (Exception exception) {
//                    logger.error(exception.getMessage());
//                    throw new RuntimeException();
//
//                }
            }
        }
    }

//    private void rollover(BlobContainerClient blobContainerClient, String blobName, int sourceFileNumber) {
//        AppendBlobClient destAppendBlobClient = blobContainerClient
//                .getBlobClient(blobName + "." + (sourceFileNumber + 1)).getAppendBlobClient();
//
//        if (destAppendBlobClient.exists()) {
//            rollover(blobContainerClient, blobName, sourceFileNumber + 1);
//        }
//
//        moveToNewBlob(blobContainerClient, blobName, sourceFileNumber, sourceFileNumber + 1);
//    }

//    private void moveToNewBlob(BlobContainerClient blobContainerClient, String blobName, int sourceFileNumber,
//                               int destFileNumber) {
//
//        AppendBlobClient sourceAppendBlobClient = blobContainerClient
//                .getBlobClient(blobName + "." + sourceFileNumber).getAppendBlobClient();
//
//        AppendBlobClient destAppendBlobClient = blobContainerClient
//                .getBlobClient(blobName + "." + destFileNumber).getAppendBlobClient();
//
//        // Create dest blob
//        createAppendBlob(destAppendBlobClient);
//
//        // Initiate lease client for source blob
//        BlobLeaseClient sourceBlobLeaseClient = new BlobLeaseClientBuilder()
//                .blobClient(sourceAppendBlobClient)
//                .buildClient();
//
//        // Acquire lease
//        sourceBlobLeaseClient.acquireLease(60);
//
//        // Copy from source blob
//        destAppendBlobClient.copyFromUrl(sourceAppendBlobClient.getBlobUrl());
//
//        // Release the lease
//        sourceBlobLeaseClient.releaseLease();
//
//        // Delete source blob
//        sourceAppendBlobClient.delete();
//    }


    private void createAppendBlobIfNotExist(AppendBlobClient appendBlobClient) {
        if (!appendBlobClient.exists()) {
            createAppendBlob(appendBlobClient);
        }
    }

    private void createAppendBlob(AppendBlobClient appendBlobClient) {

        // Set the http headers
//        BlobHttpHeaders blobHttpHeaders = new BlobHttpHeaders()
//                .setContentType("text/plain");

        // Set the request conditions like maximum blob size
        AppendBlobRequestConditions requestConditions = new AppendBlobRequestConditions()
                .setIfNoneMatch("*"); // To disable overwrite

        // Wrap all the parameters
        AppendBlobCreateOptions appendBlobCreateOptions = new AppendBlobCreateOptions()
//                .setHeaders(blobHttpHeaders)
                .setRequestConditions(requestConditions);

        // Create the response
        Response<AppendBlobItem> response = appendBlobClient.createWithResponse(
                appendBlobCreateOptions, null, Context.NONE
        );

        logger.info("Returned status code for creating blob: {}", response.getStatusCode());
    }

    public ConcurrentMap<String, Integer> getCurrentActiveIndex() {
        return currentActiveIndex;
    }
}
