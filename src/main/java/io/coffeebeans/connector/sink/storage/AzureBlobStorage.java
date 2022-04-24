package io.coffeebeans.connector.sink.storage;

import com.azure.core.util.Context;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.options.AppendBlobCreateOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import io.coffeebeans.connector.sink.exception.UnsupportedException;
import java.io.ByteArrayInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class will handle the interaction with blob storage service.
 */
public class AzureBlobStorage implements Storage {
    private static final Logger logger = LoggerFactory.getLogger(Storage.class);

    private final BlobContainerClient containerClient;

    /**
     * Constructor.
     *
     * @param connectionString Connection url string of the blob storage service
     * @param containerName Container name where data will be stored
     */
    public AzureBlobStorage(String connectionString, String containerName) {
        this.containerClient = new BlobContainerClientBuilder()
                .connectionString(connectionString)
                .containerName(containerName)
                .buildClient();
    }

    /**
     * Append the data in append blob in the container with provided blob name.
     * If append blob does not exist it will first create and then append.
     *
     * @param blobName Blob name (including the complete folder path)
     * @param data Data as byte array
     */
    @Override
    public void append(String blobName, byte[] data) {
        append(blobName, -1, data);
    }

    /**
     * Append the data in append blob in the container with provided blob name. If append blob does not exist it
     * will first create with setting the max. blob size and then append.
     *
     * @param blobName Blob name (including the complete folder path)
     * @param maxBlobSize Maximum size up to which the blob will grow
     * @param data Data as byte array
     */
    @Override
    public void append(String blobName, long maxBlobSize, byte[] data) {

        AppendBlobClient appendBlobClient = this.containerClient.getBlobClient(blobName).getAppendBlobClient();

        try {
            createIfNotExist(appendBlobClient, maxBlobSize);
            appendBlobClient.appendBlockWithResponse(
                    new ByteArrayInputStream(data), data.length, null,
                    new AppendBlobRequestConditions(), null, Context.NONE);
        } catch (Exception e) {
            logger.error("Error while performing append operation, exception: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Upload the data in the container with provided blob name. If blob does not exist it
     * will first create and then upload.
     *
     * @param blobName Blob name (including the complete folder path)
     * @param data Data as byte array
     */
    @Override
    public void upload(String blobName, byte[] data) {
        upload(blobName, -1, data);
    }

    /**
     * Upload the data in the container with provided blob name. If blob does not exist it
     * will first create and then upload.
     *
     * @param blobName Blob name (including the complete folder path)
     * @param maxBlobSize Maximum size up to which the blob will grow
     * @param data Data as byte array
     */
    @Override
    public void upload(String blobName, long maxBlobSize, byte[] data) {
        throw new UnsupportedException("Uploading to block blob is not yet supported");
    }

    private void createIfNotExist(AppendBlobClient appendBlobClient, long maxBlobSize) {
        if (appendBlobClient.exists()) {
            return;
        }
        // Create append blob
        AppendBlobRequestConditions requestConditions = new AppendBlobRequestConditions()
                .setIfNoneMatch("*"); // To disable overwrite

        if (maxBlobSize > 0) {
            requestConditions.setMaxSize(maxBlobSize);
        }
        createAppendBlob(appendBlobClient, requestConditions);
    }

    private void createAppendBlob(AppendBlobClient appendBlobClient, AppendBlobRequestConditions requestConditions) {
        AppendBlobCreateOptions appendBlobCreateOptions = new AppendBlobCreateOptions()
                .setRequestConditions(requestConditions);

        try {
            appendBlobClient.createWithResponse(appendBlobCreateOptions, null, Context.NONE);

        } catch (Exception e) {
            logger.error("Error creating append blob with exception: {}", e.getMessage());
            throw e;
        }
    }

}
