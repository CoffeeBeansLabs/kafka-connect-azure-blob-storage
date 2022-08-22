package io.coffeebeans.connect.azure.blob.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.azure.storage.blob.BlobContainerClient;
import io.coffeebeans.connect.azure.blob.util.AzureBlobStorageUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

@Tag("IntegrationTest")
public class BaseConnectorIntegrationTest {
    protected static final String CONNECTION_STRING_ENV_VARIABLE = "AZ_BLOB_CONNECTION_STRING";

    protected static final int MAX_TASKS = 3;
    protected static final long AZ_BLOB_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);

    protected static BlobContainerClient containerClient;

    protected EmbeddedConnectCluster connect;
    protected Map<String, String> props;

    @BeforeEach
    public void setup() {
        startConnect();
    }

    @AfterEach
    public void close() {
        // stop all Connect, Kafka and Zk threads.
        connect.stop();
    }

    protected void startConnect() {
        connect = new EmbeddedConnectCluster.Builder()
                .name("blob-sink-connect-cluster")
                .build();

        // start the clusters
        connect.start();
    }

    /**
     * Wait up to {@link #AZ_BLOB_TIMEOUT_MS timeout} for the connector to write the specified
     * number of blobs.
     *
     * @param expectedNumberOfBlobs    expected number of files in the bucket
     * @return the time this method discovered the connector has written the files
     * @throws InterruptedException if this was interrupted
     */
    protected long waitForBlobsInContainer(int expectedNumberOfBlobs) throws InterruptedException {
        return AzureBlobStorageUtils
                .waitForBlobsInContainer(containerClient, expectedNumberOfBlobs, AZ_BLOB_TIMEOUT_MS);
    }

    /**
     * Get a list of the expected blob names for the container.
     * <p>
     * Format: topics/it-topic/partition=5/it-topic+5+0.gzip.parquet
     *
     * @param topic      kafka topic
     * @param partition  kafka partition
     * @param flushSize  flush size
     * @param numRecords number of records produced
     * @param extension  the expected extensions of the files including compression
     * @return the list of expected filenames
     */
    protected List<String> getExpectedBlobNames(String topic,
                                                int partition,
                                                int flushSize,
                                                long numRecords,
                                                String extension) {


        int expectedBlobCount = (int) numRecords / flushSize;
        List<String> expectedBlobs = new ArrayList<>();

        for (int offset = 0; offset < expectedBlobCount * flushSize; offset += flushSize) {

            String blobPath = String.format(
                    "topics/%s/partition=%d/%s+%d+%d.%s",
                    topic,
                    partition,
                    topic,
                    partition,
                    offset,
                    extension
            );
            expectedBlobs.add(blobPath);
        }
        return expectedBlobs;
    }

    /**
     * Check if the blob names in the container have the expected namings.
     *
     * @param expectedBlobs the list of expected blob names for exact comparison
     */
    protected void assertBlobNamesValid(List<String> expectedBlobs) {

        List<String> actualBlobs = AzureBlobStorageUtils
                .getBlobNames(containerClient, AZ_BLOB_TIMEOUT_MS);

        assertThat(actualBlobs).containsExactlyInAnyOrderElementsOf(expectedBlobs);
    }
}
