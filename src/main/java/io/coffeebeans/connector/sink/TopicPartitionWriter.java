package io.coffeebeans.connector.sink;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.Partitioner;
import io.coffeebeans.connector.sink.storage.RecordWriter;
import io.coffeebeans.connector.sink.storage.RecordWriterProvider;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class TopicPartitionWriter {
    private static final Logger logger = LoggerFactory.getLogger(TopicPartitionWriter.class);

    private final int flushSize;
    private final int rotateIntervalMs;
    private final Partitioner partitioner;
    private final Queue<SinkRecord> buffer;
    private final AzureBlobSinkConfig config;
    private final TopicPartition topicPartition;
    private final ErrantRecordReporter reporter;
    private final RecordWriterProvider recordWriterProvider;

    private final Map<String, Long> startTimes;
    private final Map<String, Long> startOffsets;
    private final Map<String, Long> recordsCount;
    private final Map<String, RecordWriter> writers;

    public TopicPartitionWriter(TopicPartition topicPartition,
                                AzureBlobSinkConfig config,
                                ErrantRecordReporter reporter,
                                Partitioner partitioner,
                                RecordWriterProvider recordWriterProvider) {

        this.topicPartition = topicPartition;
        this.reporter = reporter;
        this.config = config;
        this.partitioner = partitioner;
        this.buffer = new LinkedList<>();

        this.writers = new HashMap<>();
        this.startTimes = new HashMap<>();
        this.flushSize = 2000;
        this.rotateIntervalMs = 60 * 1000;
        this.startOffsets = new HashMap<>();
        this.recordsCount = new HashMap<>();
        this.recordWriterProvider = recordWriterProvider;
    }

    public void buffer(SinkRecord sinkRecord) {
        buffer.add(sinkRecord);
    }

    public void write() {
        long now = System.currentTimeMillis();

        while (!buffer.isEmpty()) {
             // Get the Record writer using encoded partition and write the record to the writer.
            SinkRecord record = buffer.poll();
            String encodedPartition = partitioner.encodePartition(record);
            rotateIfFlushSizeConditionMet(encodedPartition);

            RecordWriter writer = writers.get(encodedPartition);

            if (writer == null) {
                // Writer does not exist so create a new one
                writer = instantiateNewWriter(record, encodedPartition);
                startTimes.put(encodedPartition, now);
            }

            try {
                writer.write(record);
                recordsCount.put(encodedPartition, recordsCount.get(encodedPartition) + 1);

            } catch (IOException e) {
                logger.error("Failed to write record with offset: {}, encodedPartition: {}, sending to DLQ",
                        record.kafkaOffset(), encodedPartition);
                reporter.report(record, e);
            }
        }
        checkForRotationAndPerform(now);
    }

    private RecordWriter instantiateNewWriter(SinkRecord record, String encodedPartition) {
        RecordWriter writer = recordWriterProvider.getRecordWriter(
                config, partitioner.generateFullPath(record, encodedPartition, record.kafkaOffset()));

        writers.put(encodedPartition, writer);
        startOffsets.put(encodedPartition, record.kafkaOffset());
        recordsCount.put(encodedPartition, 0L);
        return writer;
    }

    private void checkForRotationAndPerform(long currentTime) {
        Iterator<Map.Entry<String, RecordWriter>> iterator = writers.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, RecordWriter> entry = iterator.next();
            String encodedPartition = entry.getKey();
            if (!shouldRotate(encodedPartition, currentTime)) {
                continue;
            }

            // Perform rotation, i.e. close the current writer and remove all data for this encodedPartition
            try {
                entry.getValue().commit();

            } catch (IOException e) {
                logger.error("Failed to commit file with encodedPartition: {}, Removing the writer", encodedPartition);
            }

            iterator.remove();
            startOffsets.remove(encodedPartition);
            recordsCount.remove(encodedPartition);
            startTimes.remove(encodedPartition);
        }
    }

    private void rotateIfFlushSizeConditionMet(String encodedPartition) {
        if (!isFlushSizeConditionMet(encodedPartition)) {
            return;
        }
        commit(encodedPartition);
    }

    private void commit(String encodedPartition) {
        RecordWriter writer = writers.get(encodedPartition);
        if (writer == null) {
            logger.warn("Writer not available to commit. Ignoring");
            return;
        }
        try {
            writer.commit();

        } catch (IOException e) {
            logger.error("Failed to commit file with encodedPartition: {}, Removing the writer", encodedPartition);
        }
        writers.remove(encodedPartition);
        startOffsets.remove(encodedPartition);
        startTimes.remove(encodedPartition);
        recordsCount.remove(encodedPartition);
    }

    private boolean isFlushSizeConditionMet(String encodedPartition) {
        // If no. of records written equals or exceed the flush size then return true
        return recordsCount.get(encodedPartition) != null && recordsCount.get(encodedPartition) >= flushSize;
    }

    private boolean shouldRotate(String encodedPartition, long currentTime) {
        // If no. of records written equals or exceed the flush size then return true
        if (recordsCount.get(encodedPartition) != null && recordsCount.get(encodedPartition) >= flushSize) {
            return true;
        }
        // If difference of current and start time equals or exceed rotate interval ms then return true
        return startTimes.get(encodedPartition) != null
                && currentTime - startTimes.get(encodedPartition) >= rotateIntervalMs;
    }

    public void close() throws IOException {
        for (RecordWriter writer : writers.values()) {
            writer.close();
        }
        writers.clear();
        startOffsets.clear();
        startTimes.clear();
        recordsCount.clear();
    }
}
