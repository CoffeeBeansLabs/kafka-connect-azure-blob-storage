package io.coffeebeans.connector.sink.storage;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.AppendBlobItem;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.AppendBlobCreateOptions;
import com.azure.storage.blob.specialized.AppendBlobAsyncClient;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import com.azure.storage.common.Utility;
import com.azure.storage.common.policy.RetryPolicyType;
import io.coffeebeans.connector.sink.exception.BlobStorageException;
import io.coffeebeans.connector.sink.exception.UnsupportedException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;


/**
 * This class will handle the interaction with blob storage service.
 */
public class AzureBlobStorageManager implements StorageManager {
    private static final Logger logger = LoggerFactory.getLogger(StorageManager.class);

    private final BlobContainerClient containerClient;
    private final BlobContainerAsyncClient containerAsyncClient;

    private int retries;
    private long retryBackoffMs;
    private long retryMaxBackoffMs;
    private long connectionTimeoutMs;
    private String retryType;

    /**
     * Constructor.
     *
     * @param connectionString Connection url string of the blob storage service
     * @param containerName Container name where data will be stored
     */
    public AzureBlobStorageManager(String connectionString, String containerName) {
        this.containerClient = new BlobContainerClientBuilder()
                .connectionString(connectionString)
                .containerName(containerName)
                .buildClient();

        this.containerAsyncClient = new BlobContainerClientBuilder()
                .connectionString(connectionString)
                .containerName(containerName)
                .buildAsyncClient();
    }

    @Override
    public void configure(Map<String, Object> config) {

        this.retries = (int) config.getOrDefault("azblob.retry.retries", 3);
        this.retryBackoffMs = (long) config.getOrDefault("azblob.retry.backoff.ms", 4000L);
        this.retryMaxBackoffMs = (long) config.getOrDefault("azblob.retry.max.backoff.ms", 120000L);
        this.connectionTimeoutMs = (long) config.getOrDefault("azblob.connection.timeout.ms", 30000L);
        this.retryType = (String) config.getOrDefault("azblob.retry.type", "EXPONENTIAL");
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
     * Asynchronously append the data to the provided blob.
     *
     * @param blobName Name of the blob
     * @param data Data to be appended
     * @return Mono of response signalling success or error
     */
    @Override
    public Mono<Response<AppendBlobItem>> appendAsync(String blobName, byte[] data) {

        try {
            AppendBlobAsyncClient appendBlobAsyncClient = this.containerAsyncClient
                    .getBlobAsyncClient(blobName)
                    .getAppendBlobAsyncClient();

            // Creating Mono for creating append blob asynchronously
            Mono<Response<AppendBlobItem>> createAppendBlobResponseMono = createIfNotExistAsync(appendBlobAsyncClient)
                    .onErrorMap(e -> handleErrorForCreateAppendBlob(e, blobName));

            // Creating Mono for appending data asynchronously
            InputStream inputStream = new ByteArrayInputStream(data);

            Flux<ByteBuffer> dataFlux = Utility
                    .convertStreamToByteBuffer(inputStream, data.length, AppendBlobAsyncClient.MAX_APPEND_BLOCK_BYTES);

            byte[] md5 = MessageDigest.getInstance("MD5")
                            .digest(data);

            Mono<Response<AppendBlobItem>> appendBlockResponseMono = appendBlobAsyncClient
                    .appendBlockWithResponse(dataFlux, data.length, md5, new AppendBlobRequestConditions())
                    .timeout(
                            Duration.ofMillis(this.connectionTimeoutMs),
                            Mono.error(() -> new BlobStorageException("Timeout while appending data"))
                    )
                    .retryWhen(getRetryBackoffSpec())
                    .onErrorMap(e -> handleErrorForAppendBlob(e, blobName));

            // Chaining create and append mono.
            return createAppendBlobResponseMono
                    .then(appendBlockResponseMono);

        } catch (Exception e) {
            throw new BlobStorageException("Failed to asynchronously append block to blob: "
                    + blobName + " with message: " + e.getMessage());
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
    @Override
    public Mono<Response<Void>> stageBlockAsync(String blobName, String base64BlockId, byte[] data) {

        try {

            BlockBlobAsyncClient blockBlobAsyncClient = containerAsyncClient
                    .getBlobAsyncClient(blobName)
                    .getBlockBlobAsyncClient();

            InputStream inputStream = new ByteArrayInputStream(data);

            Flux<ByteBuffer> dataFlux = Utility
                    .convertStreamToByteBuffer(
                            inputStream, data.length, AppendBlobAsyncClient.MAX_APPEND_BLOCK_BYTES
                    );

            byte[] md5 = MessageDigest.getInstance("MD5")
                    .digest(data);

            return blockBlobAsyncClient
                    .stageBlockWithResponse(base64BlockId, dataFlux, data.length, md5, null)
                    .timeout(
                            Duration.ofMillis(this.connectionTimeoutMs),
                            Mono.error(() -> new BlobStorageException("Timeout while appending data"))
                    )
                    .retryWhen(getRetryBackoffSpec())
                    .onErrorMap(e -> handleErrorForStagingBlock(e, blobName));

        } catch (Exception e) {
            throw new BlobStorageException("STAGING: Failed to asynchronously stage block to blob with name: "
                    + blobName + " with message: " + e.getMessage());
        }
    }

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
    @Override
    public Mono<BlockBlobItem> commitBlockIdsAsync(String blobName,
                                                             List<String> base64BlockIds,
                                                             boolean overwrite) {

        BlockBlobAsyncClient blockBlobAsyncClient = containerAsyncClient
                .getBlobAsyncClient(blobName)
                .getBlockBlobAsyncClient();

        return blockBlobAsyncClient
                .commitBlockList(base64BlockIds, overwrite)
                .timeout(
                        Duration.ofMillis(this.connectionTimeoutMs),
                        Mono.error(() -> new BlobStorageException("Timeout while committing block"))
                )
                .retryWhen(getRetryBackoffSpec())
                .onErrorMap(e -> handleErrorForCommittingBlock(e, blobName));
    }

    /**
     * Synchronously create append blob if it does not exist.
     *
     * @param appendBlobClient Blocking Client for Append blob
     * @param maxBlobSize Max blob size
     */
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

    /**
     * Asynchronously create append blob if it does not exist.
     *
     * @param appendBlobAsyncClient Async client for append blob
     * @return Mono of response signalling success or error
     */
    private Mono<Response<AppendBlobItem>> createIfNotExistAsync(AppendBlobAsyncClient appendBlobAsyncClient) {

        AppendBlobRequestConditions requestConditions = new AppendBlobRequestConditions()
                .setIfNoneMatch("*");

        AppendBlobCreateOptions appendBlobCreateOptions = new AppendBlobCreateOptions()
                .setRequestConditions(requestConditions);

        return appendBlobAsyncClient.createIfNotExistsWithResponse(appendBlobCreateOptions)
                .timeout(
                        Duration.ofMillis(this.connectionTimeoutMs)
                )
                .retryWhen(getRetryBackoffSpec());
    }

    /**
     * Synchronously creates append blob.
     *
     * @param appendBlobClient Append blob client specific to an append blob
     * @param requestConditions Request conditions
     */
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

    /**
     * Configure RetryBackoffSpec based on the configured retry parameters.
     *
     * @return RetryBackoffSpec
     */
    private RetryBackoffSpec getRetryBackoffSpec() {
        boolean isRetryTypeFixed = RetryPolicyType.FIXED
                .toString()
                .equalsIgnoreCase(this.retryType);

        // For FIXED retry type
        if (isRetryTypeFixed) {

            return Retry.fixedDelay(this.retries, Duration.ofMillis(this.retryBackoffMs))
                    .doAfterRetry(retrySignal ->
                            logger.warn("CREATE: Retrying at " + LocalTime.now()
                                    + ", attempt: " + retrySignal.totalRetries())
                    )
                    .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) ->
                            Exceptions.propagate(retrySignal.failure())
                    );
        }

        // For EXPONENTIAL retry type
        return Retry.backoff(this.retries, Duration.ofMillis(this.retryBackoffMs))
                .maxBackoff(Duration.ofMillis(this.retryMaxBackoffMs))
                .jitter(0.5d)
                .doAfterRetry(retrySignal ->
                        logger.warn("CREATE: Retrying at " + LocalTime.now()
                                + ", attempt: " + retrySignal.totalRetries())
                )
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) ->
                        Exceptions.propagate(retrySignal.failure())
                );
    }

    private Throwable handleErrorForCreateAppendBlob(Throwable throwable, String blobName) {

        logger.error("CREATE: Failed to create blob with name: " + blobName);
        return new BlobStorageException("CREATE: Failed to create blob with name: "
                + blobName + " with error message: " + throwable.getMessage());
    }

    private Throwable handleErrorForAppendBlob(Throwable throwable, String blobName) {

        logger.error("APPEND: Failed to append block with name: " + blobName);
        return new BlobStorageException("APPEND: Failed to append block in blob: "
                + blobName + " with error message: " + throwable.getMessage());
    }

    private Throwable handleErrorForStagingBlock(Throwable throwable, String blobName) {

        logger.error("STAGING: Failed to stage block on blob with name: " + blobName);
        return new BlobStorageException("STAGING: Failed to stage block in blob: "
                + blobName + " with error message: " + throwable.getMessage());
    }

    private Throwable handleErrorForCommittingBlock(Throwable throwable, String blobName) {

        logger.error("COMMITTING: Failed to commit blocks on blob with name: " + blobName);
        return new BlobStorageException("COMMITTING: Failed to commit blocks in blob: "
                + blobName + " with error message: " + throwable.getMessage());
    }
}
