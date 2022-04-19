package io.coffeebeans.connector.sink.storage;

/**
 * Determine, store and return the Storage class
 */
public class StorageFactory {
    public static StorageManager storageManager;

    public void configure(String connectionString, String containerName) {
        storageManager = new BlobStorageManager(connectionString, containerName);
    }

    public static StorageManager getStorageManager() {
        return storageManager;
    }
}
