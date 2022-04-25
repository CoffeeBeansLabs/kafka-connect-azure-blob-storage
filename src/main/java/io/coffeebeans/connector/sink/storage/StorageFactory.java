package io.coffeebeans.connector.sink.storage;

/**
 * Initialize new AzureBlobStorage instance.
 */
public class StorageFactory {

    private static Storage storage;

    /**
     * Initialize new AzureBlobStorage instance.
     *
     * @param connectionString Azure Blob Storage service connection string
     * @param containerName Container name where data will be stored
     */
    public static void set(String connectionString, String containerName) {
        storage = new AzureBlobStorage(connectionString, containerName);
    }

    /**
     * Nullable.
     * Get the Storage instance.
     *
     * @return Storage
     */
    public static Storage get() {
        return storage;
    }
}
