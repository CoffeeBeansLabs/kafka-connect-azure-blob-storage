package io.coffeebeans.connector.sink.storage;

/**
 * Initialize new AzureBlobStorage instance.
 */
@Deprecated
public class StorageFactory {

    private static StorageManager storageManager;

    /**
     * Initialize new AzureBlobStorage instance.
     *
     * @param connectionString Azure Blob Storage service connection string
     * @param containerName Container name where data will be stored
     */
    public static void set(String connectionString, String containerName) {
        storageManager = new AzureBlobStorageManager(connectionString, containerName);
    }

    /**
     * Nullable.
     * Get the Storage instance.
     *
     * @return Storage
     */
    public static StorageManager get() {
        return storageManager;
    }
}
