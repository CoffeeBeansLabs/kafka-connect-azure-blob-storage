package io.coffeebeans.connector.sink.storage;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.LeaseStateType;
import com.azure.storage.blob.options.AppendBlobCreateOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.model.Metadata;
import java.io.ByteArrayInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager class to manage all the metadata related operations.
 */
public class MetadataManager {
    private static final Logger logger = LoggerFactory.getLogger(MetadataManager.class);
    private static final int LEASE_DURATION = 60;
    public static final String FILE_NAME = "_metadata_";
    public static final String FOLDER_DELIMITER = "/";

    /**
     * Retrieve metadata object.
     *
     * @param containerClient Container client
     * @param path folder path
     * @return Metadata object
     * @throws JsonProcessingException json processing exception
     */
    public static Metadata retrieveMetadata(BlobContainerClient containerClient, String path) throws
            JsonProcessingException {

        // Get the complete metadata as string
        String metadataContent = getMetadataFileContentsAsString(containerClient, path);
        String latestMetadata;

        int delimiterIndex = metadataContent.lastIndexOf("}{");
        if (delimiterIndex != -1) {
            // Get the last appended metadata that contains the last active index
            latestMetadata = metadataContent.substring(delimiterIndex + 1);

        } else {
            // If not found that means only one metadata is present.
            latestMetadata = metadataContent;
        }

        try {
            return new ObjectMapper().readValue(latestMetadata, Metadata.class);
        } catch (JsonProcessingException exception) {
            logger.error("Unable to convert metadata string to object");
            throw exception;
        }
    }

    /**
     * I fetch the metadata file contents from blob storage service and convert it to string and return it.
     *
     * @param containerClient Blob Container Client
     * @param path folder path
     * @return String value of metadata
     */
    public static String getMetadataFileContentsAsString(BlobContainerClient containerClient, String path) {

        // Create file name
        String metadataFileName = path + FOLDER_DELIMITER + FILE_NAME;

        // Get append blob client
        AppendBlobClient metadataBlobClient = containerClient.getBlobClient(metadataFileName).getAppendBlobClient();

        try {
            // Fetch the binary data from metadata file
            BinaryData binaryContent = metadataBlobClient.downloadContent();
            byte[] binaryMetadata = binaryContent.toBytes();

            // Convert binary to string
            return new String(binaryMetadata);

        } catch (Exception e) {
            logger.error("Failed to get metadata file contents");
            throw e;
        }
    }

    /**
     * I append the metadata to the metadata blob. I will create the metadata blob if it does not exist.
     *
     * @param containerClient Container client
     * @param path folder path
     * @param metadata metadata file
     */
    public static void appendMetadata(BlobContainerClient containerClient, String path, Metadata metadata) {
        // Create file name
        String metadataFileName = path + FOLDER_DELIMITER + FILE_NAME;

        // Get append blob client
        AppendBlobClient metadataBlobClient = containerClient.getBlobClient(metadataFileName).getAppendBlobClient();
        ObjectMapper objectMapper = new ObjectMapper();

        BlobLeaseClient metadataBlobLeaseClient;
        try {
            // Check if metadata file already exists, if not then create a new file.
            if (!isMetadataExists(metadataBlobClient)) {
                createMetadataFile(metadataBlobClient, metadataFileName);
            }


            metadataBlobLeaseClient = new BlobLeaseClientBuilder()
                    .blobClient(metadataBlobClient)
                    .buildClient();

            // Append to metadata blob
            byte[] metadataBytes = objectMapper.writeValueAsBytes(metadata);

            // Check lease status of metadata blob
            if (metadataBlobClient.getProperties().getLeaseState().equals(LeaseStateType.LEASED)) {

                // If leased then continue to poll lease status
                do {
                    Thread.sleep(1000);

                    // Once lease is available break
                } while (!metadataBlobClient.getProperties().getLeaseState().equals(LeaseStateType.AVAILABLE));

                // Check if metadata is already updated by another connector
                Metadata retrievedMetadata = retrieveMetadata(containerClient, path);
                if (retrievedMetadata.equals(metadata)) {
                    return;
                }
            }

            // Acquire lease for the metadata blob
            String leaseId = metadataBlobLeaseClient.acquireLease(LEASE_DURATION);

            AppendBlobRequestConditions requestConditions = new AppendBlobRequestConditions().setLeaseId(leaseId);

            // Perform append operation
            // metadataBlobClient.appendBlock(new ByteArrayInputStream(metadataBytes), metadataBytes.length);
            metadataBlobClient.appendBlockWithResponse(
                    new ByteArrayInputStream(metadataBytes), metadataBytes.length,
                    null, requestConditions, null, Context.NONE
            );

            // Release lease
            metadataBlobLeaseClient.releaseLease();

        } catch (JsonProcessingException jsonProcessingException) {
            logger.error("Unable to serialize metadata");
            logger.error("Unable to append metadata");

        } catch (Exception e) {
            logger.error("Unable to append metadata");
        }
    }

    /**
     * I check if the metadata file already exists on that path or not.
     *
     * @param metadataBlobClient AppendBlobClient
     * @return true if file exists otherwise false
     */
    public static boolean isMetadataExists(AppendBlobClient metadataBlobClient) {
        try {
            // Check if file exists
            return metadataBlobClient.exists();

        } catch (Exception e) {
            logger.error("Unable to check if the metadata file exists or not.");
            throw e;
        }
    }

    /**
     * I create a append blob to store the meta data.
     *
     * @param metadataBlobClient AppendBlobClient
     * @param metadataFileName metadata file name with folder path
     */
    public static void createMetadataFile(AppendBlobClient metadataBlobClient, String metadataFileName) {

        // Configure http headers and set the content-type to text/plain
        BlobHttpHeaders httpHeaders = new BlobHttpHeaders().setContentType("text/plain");

        // Configure append blob creation request conditions and disable overwrite
        AppendBlobRequestConditions requestConditions = new AppendBlobRequestConditions().setIfNoneMatch("*");

        // Wrap all the parameters
        AppendBlobCreateOptions appendBlobCreateOptions = new AppendBlobCreateOptions()
                .setHeaders(httpHeaders)
                .setRequestConditions(requestConditions);

        try {
            // Create the response
            metadataBlobClient.createWithResponse(
                    appendBlobCreateOptions, null, Context.NONE
            );

        } catch (Exception e) {
            logger.error("Unable to create metadata file: " + metadataFileName);
            logger.error(e.getMessage());
        }
    }
}
