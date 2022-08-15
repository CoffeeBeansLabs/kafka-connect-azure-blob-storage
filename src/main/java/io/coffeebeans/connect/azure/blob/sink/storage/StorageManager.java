package io.coffeebeans.connect.azure.blob.sink.storage;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.models.AppendBlobItem;
import com.azure.storage.blob.models.BlockBlobItem;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Mono;

/**
 * This will be responsible for calling creation, append, and upload APIs of the storage service.
 */
public interface StorageManager {

    void configure(Map<String, Object> configProps);

    /**
     * I will append the data in append blob in the container with provided blob name. If append blob does not exist it
     * will first create and then append.
     *
     * @param blobName - Blob name (including the complete folder path)
     * @param data - Data as byte array
     */
    void append(String blobName, byte[] data);

    /**
     * I will append the data in append blob in the container with provided blob name. If append blob does not exist it
     * will first create with setting the max. blob size and then append.
     *
     * @param blobName - Blob name (including the complete folder path)
     * @param maxBlobSize - Maximum size up to which the blob will grow
     * @param data - Data as byte array
     */
    void append(String blobName, long maxBlobSize, byte[] data);

    /**
     * Asynchronously append the data to the provided blob.
     *
     * @param blobName Name of the blob
     * @param data Data to be appended
     * @return Mono of response signalling success or error
     */
    Mono<Response<AppendBlobItem>> appendAsync(String blobName, byte[] data);

    /**
     * I will upload the data in the container with provided blob name. If blob does not exist it
     * will first create and then upload.
     *
     * @param blobName - Blob name (including the complete folder path)
     * @param data - Data as byte array
     */
    void upload(String blobName, byte[] data);

    /**
     * I will upload the data in the container with provided blob name. If blob does not exist it
     * will first create and then upload.
     *
     * @param blobName - Blob name (including the complete folder path)
     * @param maxBlobSize - Maximum size up to which the blob will grow
     * @param data - Data as byte array
     */
    void upload(String blobName, long maxBlobSize, byte[] data);

    /**
     * It uploads the data asynchronously to the blob storage
     * where it stays in the staging area until commit operation
     * is invoked on that blob.
     *
     * <p>All the block ids in a blob should have same length.
     *
     * @param blobName Name of the blob
     * @param base64BlockId Base64 encoded block id
     * @param data byte array of the data to be uploaded
     * @return Mono of response
     */
    Mono<Response<Void>> stageBlockAsync(String blobName, String base64BlockId, byte[] data);

    /**
     * It commits the pre-staged blocks to the blob. All the
     * blocks will be committed to block blob in the order
     * provided in the list of block ids.
     *
     * <p>A part of block blob can be changed by providing the new
     * block id with the existing block ids and setting the
     * overwrite flag.
     *
     * @param blobName Blob Name of the blob
     * @param base64BlockIds List of base64 encoded block ids
     * @param overwrite Whether to overwrite block or not
     * @return Mono of response signalling success or error
     */
    Mono<BlockBlobItem> commitBlockIdsAsync(String blobName, List<String> base64BlockIds, boolean overwrite);
}
