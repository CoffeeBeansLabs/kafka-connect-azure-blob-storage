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
public class BlobStorageManager implements StorageManager {
    private static final Logger logger = LoggerFactory.getLogger(StorageManager.class);

    private final BlobContainerClient containerClient;

    /**
     * Constructor with connection url of the blob storage service.
     *
     * @param connectionString Connection url string of the blob storage service
     */
    public BlobStorageManager(String connectionString, String containerName) {
        this.containerClient = new BlobContainerClientBuilder()
                .connectionString(connectionString)
                .containerName(containerName)
                .buildClient();
    }

    /**
     * I will append the data in append blob in the container with provided blob name. If append blob does not exist it
     * will first create and then append.
     *
     * @param blobName - Blob name (including the complete folder path)
     * @param data - Data as byte array
     */
    @Override
    public void append(String blobName, byte[] data) {
        append(blobName, -1, data);
    }

    /**
     * I will append the data in append blob in the container with provided blob name. If append blob does not exist it
     * will first create with setting the max. blob size and then append.
     *
     * @param blobName - Blob name (including the complete folder path)
     * @param maxBlobSize - Maximum size up to which the blob will grow
     * @param data - Data as byte array
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
     * I will upload the data in the container with provided blob name. If blob does not exist it
     * will first create and then upload.
     *
     * @param blobName - Blob name (including the complete folder path)
     * @param data - Data as byte array
     */
    @Override
    public void upload(String blobName, byte[] data) {
        upload(blobName, -1, data);
    }

    /**
     * I will upload the data in the container with provided blob name. If blob does not exist it
     * will first create and then upload.
     *
     * @param blobName - Blob name (including the complete folder path)
     * @param maxBlobSize - Maximum size up to which the blob will grow
     * @param data - Data as byte array
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
