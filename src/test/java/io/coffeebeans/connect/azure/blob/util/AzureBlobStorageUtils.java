package io.coffeebeans.connect.azure.blob.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBlobStorageUtils {
    private static final Logger log = LoggerFactory.getLogger(AzureBlobStorageUtils.class);

    /**
     * Wait for blobs to be written in storage and assert no. of blobs written.
     *
     * @param containerClient container client
     * @param expectedNumberOfBlobs expected number of blobs
     * @param timeoutMs timeout in ms
     * @return Time at which it asserted no. of blobs
     * @throws InterruptedException If it was interrupted
     */
    public static long waitForBlobsInContainer(BlobContainerClient containerClient,
                                               int expectedNumberOfBlobs,
                                               long timeoutMs) throws InterruptedException {

        TestUtils.waitForCondition(
                () -> assertBlobCountInContainer(containerClient, expectedNumberOfBlobs, timeoutMs),
                timeoutMs,
                "Blobs not written in given time"
        );
        return System.currentTimeMillis();
    }

    /**
     * Asserts number of blobs in container.
     *
     * @param containerClient Container client
     * @param expectedNumberOfBlobs Expected number of blobs
     * @param timeoutMs Timeout in ms
     * @return True if assertion is successful else false
     */
    private static boolean assertBlobCountInContainer(BlobContainerClient containerClient,
                                                      int expectedNumberOfBlobs,
                                                      long timeoutMs) {

        try {
            int actualNumberOfBlobs = getContainerBlobCount(containerClient, timeoutMs);
            assertEquals(expectedNumberOfBlobs, actualNumberOfBlobs);
            return true;

        } catch (Exception e) {
            log.error("Failed to assert no. of blobs in container: {}", containerClient.getBlobContainerName());
            return false;
        }
    }

    /**
     * Gets Number of files in the container.
     *
     * @param containerClient ContainerClient
     * @param timeoutMs Timeout in milliseconds
     * @return Number of files
     */
    private static int getContainerBlobCount(BlobContainerClient containerClient, long timeoutMs) {

        AtomicInteger totalBlobsInContainer = new AtomicInteger();

        PagedIterable<BlobItem> blobs = containerClient
                .listBlobs(new ListBlobsOptions(), Duration.ofMillis(timeoutMs));

        blobs.forEach(
                blobItem -> totalBlobsInContainer.getAndIncrement()
        );

        return totalBlobsInContainer.get();
    }

    public static List<String> getBlobNames(BlobContainerClient containerClient, long timeoutMs) {

        List<String> blobNames = new ArrayList<>();

        PagedIterable<BlobItem> blobs = containerClient
                .listBlobs(new ListBlobsOptions(), Duration.ofMillis(timeoutMs));

        blobs.forEach(
                blobItem -> blobNames.add(blobItem.getName())
        );

        return blobNames;
    }
}
