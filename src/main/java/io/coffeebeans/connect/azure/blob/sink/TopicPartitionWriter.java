package io.coffeebeans.connect.azure.blob.sink;

import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connect.azure.blob.sink.exception.PartitionException;
import io.coffeebeans.connect.azure.blob.sink.exception.SchemaParseException;
import io.coffeebeans.connect.azure.blob.sink.format.Format;
import io.coffeebeans.connect.azure.blob.sink.format.RecordWriter;
import io.coffeebeans.connect.azure.blob.sink.format.SchemaStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TopicPartitionWriter will be unique for each topic-partition. It will be
 * responsible for batching the records, performing rotation, and maintaining offsets.
 * It will also maintain a map of writers unique to each encoded partition.
 */
public class TopicPartitionWriter {
    private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriter.class);

    private final int flushSize;
    private Long lastSuccessfulOffset;
    private final long rotationIntervalMs;
    private final Queue<SinkRecord> buffer;
    private boolean isSchemaStoreConfigurationChecked;
    private final AzureBlobSinkConnectorContext context;

    private final Map<String, Long> startTimes;
    private final Map<String, Long> recordsCount;
    private final Map<String, RecordWriter> writers;

    /**
     * Constructor.
     *
     * @param azureBlobSinkConnectorContext Context object
     */
    public TopicPartitionWriter(AzureBlobSinkConnectorContext azureBlobSinkConnectorContext) {

        this.context = azureBlobSinkConnectorContext;
        AzureBlobSinkConfig config = azureBlobSinkConnectorContext.getConfig();

        this.lastSuccessfulOffset = null;
        this.buffer = new LinkedList<>();
        this.flushSize = config.getFlushSize();
        this.isSchemaStoreConfigurationChecked = false;
        this.rotationIntervalMs = config.getRotateIntervalMs();

        this.writers = new HashMap<>();
        this.startTimes = new HashMap<>();
        this.recordsCount = new HashMap<>();
    }

    /**
     * Buffer. Add record to the buffer queue.
     *
     * @param sinkRecord Record to be put into buffer
     */
    public void buffer(SinkRecord sinkRecord) {
        buffer.add(sinkRecord);
    }

    /**
     * It polls records from buffer and write it using RecordWriter.
     * It also checks for rotation before and after record is written.
     */
    public void write() {
        long now = System.currentTimeMillis();

        while (!buffer.isEmpty()) {
            SinkRecord record = buffer.poll();

            try {
                String encodedPartition = context.encodePartition(record);
                try {

                    RecordWriter writer = writers.get(encodedPartition);

                    if (writer == null) {
                        // Writer does not exist so create a new one
                        writer = instantiateNewWriter(record, encodedPartition);
                    }

                    configureSchemaStore(context, record);
                    writer.write(record);

                    /*
                    Start time should only be stored for that encoded
                    partition when the first record has been successfully
                    written.
                     */
                    startTimes.putIfAbsent(encodedPartition, now);
                    recordsCount.put(encodedPartition, recordsCount.getOrDefault(encodedPartition, 0L) + 1);
                    lastSuccessfulOffset = record.kafkaOffset();

                    rotateIfFlushConditionMet(encodedPartition);

                } catch (RetriableException e) {
                    log.error("Failed to write record with offset: {}, encodedPartition: {}, sending to DLQ",
                            record.kafkaOffset(),
                            encodedPartition
                    );
                    context.sendToDeadLetterQueue(record, e);
                }
            } catch (PartitionException e) {

                log.error("Failed to encode partition for a record, sending it to DLQ");
                context.sendToDeadLetterQueue(record, e);
            }
        }
        rotateIfRotateIntervalMsConditionMet(now);
    }

    /**
     * Instantiate a new RecordWriter.
     *
     * @param record Record to be processed
     * @param encodedPartition encoded partition
     * @return Record writer to write the record
     */
    private RecordWriter instantiateNewWriter(SinkRecord record, String encodedPartition) {

        String outputFileName = context.generateFullPath(
                record,
                encodedPartition,
                record.kafkaOffset()
        );
        RecordWriter writer = context.getRecordWriter(
                record.topic(),
                outputFileName
        );

        writers.put(encodedPartition, writer);
        recordsCount.put(encodedPartition, 0L);
        return writer;
    }

    /**
     * If the flush size condition is met then rotation will be done.
     *
     * @param encodedPartition encoded partition
     */
    private void rotateIfFlushConditionMet(String encodedPartition) {
        if (isFlushSizeConditionMet(encodedPartition)) {
            commit(encodedPartition);
            this.writers.remove(encodedPartition);
        }
    }

    /**
     * Iterate through the record writers and their start times. If the
     * difference between the start time and current time is more than
     * rotate interval then perform rotation.
     *
     * @param currentTime current server time
     */
    private void rotateIfRotateIntervalMsConditionMet(long currentTime) {
        if (rotationIntervalMs <= 0) {
            // Condition to check if rotation based on time is enabled or not.
            return;
        }

        List<String> writersToRemove = new ArrayList<>();
        this.writers.forEach((encodedPartition, writer) -> {

            if (!isRotateIntervalMsConditionMet(encodedPartition, currentTime)) {
                return;
            }
            commit(encodedPartition);
            writersToRemove.add(encodedPartition);
        });

        writersToRemove.forEach(this.writers::remove);
    }

    /**
     * Invoked while performing rotation (rolling file).
     * It commits the RecordWriter but does not remove the RecordWriter
     * from encodedPartition-writer mapping.
     *
     * @param encodedPartition encoded partition
     */
    private void commit(String encodedPartition) {
        RecordWriter writer = writers.get(encodedPartition);
        if (writer == null) {
            log.warn("Writer not available to commit. Ignoring");
            return;
        }
        try {

            /*
            This method is called for partitioning.
            It is safe to disable the ensureCommitted
            flag.
             */
            writer.commit();

        } catch (RetriableException e) {
            log.error("Failed to commit file with encodedPartition: {}, Removing the writer", encodedPartition);
        }
        startTimes.remove(encodedPartition);
        recordsCount.remove(encodedPartition);
    }

    /**
     * If the flush condition is met or not. Flush size is the
     * number of records a RecordWriter should process after
     * which the rotation should happen.
     *
     * @param encodedPartition encoded partition
     * @return Whether the flush size condition is met or not
     */
    private boolean isFlushSizeConditionMet(String encodedPartition) {
        if (flushSize < 0) {
            return false;
        }

        // If no. of records written equals or exceed the flush size then return true
        return recordsCount.get(encodedPartition) != null && recordsCount.get(encodedPartition) >= flushSize;
    }

    /**
     * If rotate interval ms condition is met or not.
     * Rotate interval ms is the time up to which the RecordWriter
     * is kept open. After that time is passed the writer will be
     * committed.
     *
     * @param encodedPartition encoded partition
     * @param currentTime current server time
     * @return Whether the condition is met or not
     */
    private boolean isRotateIntervalMsConditionMet(String encodedPartition, long currentTime) {
        return startTimes.get(encodedPartition) != null
                && currentTime - startTimes.get(encodedPartition) >= rotationIntervalMs;
    }

    /**
     * Invoked to close all the RecordWriters and clear mappings.
     */
    public void close() {
        for (RecordWriter writer : writers.values()) {

            /*
            This method is called when connector or
            task is deleted. So ensureCommitted flag
            has to be set.
             */
            writer.commit();
        }
        writers.clear();
        startTimes.clear();
        recordsCount.clear();
    }

    /**
     * Returns offset of last record successfully written.
     *
     * @return offset value
     */
    public Long getLastSuccessfulOffset() {
        Long offset = lastSuccessfulOffset;
        lastSuccessfulOffset = null;

        return offset;
    }

    private void configureSchemaStore(AzureBlobSinkConnectorContext context, SinkRecord record)
            throws SchemaParseException {

        if (isSchemaStoreConfigurationChecked) {
            return;
        }
        if (!isSchemaStoreRecommended(record, context.getConfig())) {
            isSchemaStoreConfigurationChecked = true;
            return;
        }
        context.configureSchemaStore();
        isSchemaStoreConfigurationChecked = true;
    }

    /**
     * The {@link SchemaStore} is only required for.<br>
     * following file formats: <br>
     * <ul>
     *     <li>{@link Format#PARQUET Parquet}</li>
     *     <li>{@link Format#AVRO Avro}</li>
     * </ul>
     *
     * <p>Additionally, it is only required for following values: <br>
     * <ul>
     *     <li>Json string (Instance of {@link String})</li>
     *     <li>Json without embedded schema or schema registry (Instance of {@link Map})</li>
     * </ul>
     *
     * @param kafkaRecord Kafka record to process
     * @param config Connector configuration
     * @return True if {@link SchemaStore} is recommended else false.
     */
    private boolean isSchemaStoreRecommended(SinkRecord kafkaRecord, AzureBlobSinkConfig config) {
        String fileFormat = config.getFormat();

        /*
        Check for supported file formats
         */
        boolean isParquetFormat = Format.PARQUET.toString().equalsIgnoreCase(fileFormat);
        boolean isAvroFormat = Format.AVRO.toString().equalsIgnoreCase(fileFormat);

        if (!isParquetFormat && !isAvroFormat) {
            return false;
        }

        /*
        Check for supported values types
         */
        boolean isInstanceOfString = kafkaRecord.value() instanceof String;
        boolean isInstanceOfMap = kafkaRecord.value() instanceof Map;

        return isInstanceOfString || isInstanceOfMap;
    }
}
