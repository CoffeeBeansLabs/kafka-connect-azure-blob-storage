package io.coffeebeans.connector.sink.storage;

/**
 * This will be responsible for calling creation, append, and upload APIs of the storage service.
 */
public interface StorageManager {

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

}
