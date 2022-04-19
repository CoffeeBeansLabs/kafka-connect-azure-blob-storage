package io.coffeebeans.connector.sink.format.parquet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.FormatManager;
import io.coffeebeans.connector.sink.format.FormatWriter;
import io.coffeebeans.connector.sink.format.avro.AvroSchemaBuilder;
import io.coffeebeans.connector.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connector.sink.storage.StorageManager;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will buffer and write data in parquet file format.
 */
public class ParquetFormatManagerOld implements FormatManager {
    private static final Logger logger = LoggerFactory.getLogger(FormatManager.class);
    private static final String FILE_FORMAT_EXTENSION = ".parquet";

    private final long bufferLength;
    private final int bufferTimeout;
    private final String containerName;
    private final ObjectMapper objectMapper;
    private final StorageManager storageManager;
    private final AvroSchemaBuilder avroSchemaBuilder;
    private final ParquetFormatWriterProviderOld formatWriterProvider;
    public static ConcurrentMap<String, Integer> activeFileIndexMap; // Key -> full path, value -> file index
    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
    private final ConcurrentMap<String, ScheduledFuture<?>> timeoutTasks; // Key -> full path, value -> scheduled future

    /**
     * Constructor.
     *
     * @param config AzureBlobSinkConfig
     * @param storageManager StorageManager
     */
    public ParquetFormatManagerOld(AzureBlobSinkConfig config, StorageManager storageManager) {
        this.bufferLength = config.getBufferLength();
        this.bufferTimeout = config.getBufferTimeout();
        this.containerName = config.getContainerName();
        this.objectMapper = new ObjectMapper();
        this.storageManager = storageManager;
        this.avroSchemaBuilder = new AvroSchemaBuilder(objectMapper);
        this.formatWriterProvider = new ParquetFormatWriterProviderOld();
        this.activeFileIndexMap = new ConcurrentHashMap<>();
        this.timeoutTasks = new ConcurrentHashMap<>();

        this.scheduledThreadPoolExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(
                config.getBufferTimeoutTaskPoolSize()
        );
        this.scheduledThreadPoolExecutor.setRemoveOnCancelPolicy(true);
    }

    /**
     * I buffer the data passed as the parameter and upload it to the storage service when the buffer size/length limit
     * is met or timeout happens whichever is earlier.
     *
     * @param fullPath Full path of the blob (this includes the blob nome)
     * @param data - value map
     * @throws IOException - Thrown if exception occur during writing of record
     * @throws InterruptedException - Thrown if exception occur during updating metadata
     */
    public void buffer(String fullPath, Map<String, Object> data) throws IOException, InterruptedException {
        ParquetFormatWriterOld writer = (ParquetFormatWriterOld) formatWriterProvider.get(fullPath);

        try {
            if (writer == null) {
                writer = instantiateNewFormatWriter(data);
                formatWriterProvider.put(fullPath, writer);
                writer.instantiateNewParquetWriter(fullPath);
            }

            writer.write(data);

            // If this record is the first in the batch then schedule the timeout task.
            if (writer.recordsWritten() == 1) {

                ScheduledFuture<?> future = scheduledThreadPoolExecutor.schedule(
                        new ParquetBufferTimeoutTaskOld(fullPath, this),
                        this.bufferTimeout, TimeUnit.SECONDS
                );

                this.timeoutTasks.put(fullPath, future);
                logger.info("Timeout task scheduled for {} writer", fullPath);
            }

            // If the total records written is equal to the buffer length then write the data
            if (writer.recordsWritten() >= bufferLength) {
                logger.info("Buffer size met, initiating write");
                write(fullPath, false);
            }

        } catch (Exception e) {
            logger.error("Failed to buffer record with exception: {}", e.getMessage());
            throw e;
        }
    }

    private ParquetFormatWriterOld instantiateNewFormatWriter(Map<String, Object> data) throws IOException {
        // Build the avro schema
        Schema avroSchema;

        try {
            JsonNode jsonNode = objectMapper.valueToTree(data);

            avroSchema = avroSchemaBuilder.buildAvroSchema(jsonNode);
            return new ParquetFormatWriterOld(avroSchema);

        } catch (IllegalArgumentException e) {
            logger.error("Failed to instantiating new format writer");
            logger.error("Error converting data to JsonNode with exception: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("Failed to instantiating new format writer");
            throw e;
        }
    }

    /**
     * I will be called by the buffer method when the buffer size/length is met or by the scheduled timeout task to
     * write the data to storage service and update the metadata.
     *
     * @param fullPath - Blob full path
     * @param invokedByTimeoutTask - Is invoked by the timeout task or by the buffer method.
     * @throws IOException - Thrown if exception occur during writing of record
     * @throws InterruptedException - Thrown if exception occur during updating metadata
     */
    protected synchronized void write(String fullPath, boolean invokedByTimeoutTask) throws IOException,
            InterruptedException {

        FormatWriter writer = formatWriterProvider.get(fullPath);
        if (writer == null) {
            return;
        }

        try {
            byte[] bytes;
            boolean shouldUpdateIndex = false;

            if (writer.recordsWritten() > 14 || invokedByTimeoutTask) {
                writer.close();
                bytes = writer.toByteArray();

                // Delete the ParquetWriter as it has already been closed
                formatWriterProvider.remove(fullPath);
                shouldUpdateIndex = true;
            } else {
                bytes = writer.toByteArray();
            }

            if (!activeFileIndexMap.containsKey(fullPath)) {
                int index = 0;
                activeFileIndexMap.put(fullPath, index);
            }

            int activeIndex = activeFileIndexMap.get(fullPath);
            String blobName = fullPath + DefaultPartitioner.FILE_DELIMITER + activeIndex + FILE_FORMAT_EXTENSION;
            this.storageManager.append(blobName, bytes);

            if (shouldUpdateIndex) {

                // Now update the file index as the parquet file is already written and no data should be appended to that
                activeFileIndexMap.put(fullPath, activeIndex + 1);
            }

            if (invokedByTimeoutTask) {
                return;
            }

            // Since it is not invoked by timeout task, the scheduled timeout task should be cancelled.
            if (shouldUpdateIndex) {
                timeoutTasks.get(fullPath).cancel(true);
            }

        } catch (Exception e) {
            logger.error("Failed to write parquet file with exception: {}", e.toString());
            throw e;
        }
    }

    public ConcurrentMap<String, Integer> getActiveFileIndexMap() {
        return activeFileIndexMap;
    }
}
