package io.coffeebeans.connector.sink;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.parquet.ParquetRecordWriterProvider;
import io.coffeebeans.connector.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import io.coffeebeans.connector.sink.partitioner.Partitioner;
import io.coffeebeans.connector.sink.partitioner.field.FieldPartitioner;
import io.coffeebeans.connector.sink.partitioner.time.TimePartitioner;
import io.coffeebeans.connector.sink.storage.BlobStorageManager;
import io.coffeebeans.connector.sink.storage.RecordWriterProvider;
import io.coffeebeans.connector.sink.storage.StorageFactory;
import io.coffeebeans.connector.sink.util.Version;

import java.io.IOException;
import java.util.*;

import io.confluent.connect.avro.AvroData;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will be initialized by the SinkConnector class and will be passed with configuration
 * props.
 */
public class AzureBlobSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(AzureBlobSinkTask.class);

    private SinkTaskContext context;
    private Partitioner partitioner;
    private AzureBlobSinkConfig config;
    private ErrantRecordReporter reporter;
    private RecordWriterProvider recordWriterProvider;
    private Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> configProps) {
        logger.info("Starting Sink Task ....................");
        config = new AzureBlobSinkConfig(configProps);
        this.topicPartitionWriters = new HashMap<>();
        this.partitioner = getPartitioner();

        StorageFactory.storageManager = new BlobStorageManager(config.getConnectionString(), config.getContainerName());
        this.reporter = this.context.errantRecordReporter();
        recordWriterProvider = new ParquetRecordWriterProvider(new AvroData(20));
        open(context.assignment());

        logger.info("Sink Task started successfully ....................");
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.isEmpty()) {
            return;
        }

        long startTime = System.nanoTime();

        List<SinkRecord> records = new ArrayList<>(collection);
        logger.info("Received {} records", records.size());

        // Loop through each record and store it in the blob storage.
        for (SinkRecord record : records) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
            if (record.value() == null) {
                continue;
            }
            topicPartitionWriters.get(topicPartition).buffer(record);
        }

        for (TopicPartitionWriter writer : topicPartitionWriters.values()) {
            try {
                writer.write();
            } catch (Exception e) {
                logger.error("Failed to process record with exception: {}", e.getMessage());
            }
        }

        logger.info("Processed {} records in {} ns time", collection.size(), System.nanoTime() - startTime);
    }

    @Override
    public void stop() {
        logger.info("Stopping Sink Task ...................");
    }

    @Override
    public void initialize(SinkTaskContext context) {
        this.context = context;

        logger.info("Checking assignments ............................");
        logger.info("Total assignments are => " + this.context.assignment().size());

        this.context.assignment().forEach((topicPartition) -> logger.info(
                topicPartition.topic() + ": " + topicPartition.partition())
        );
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        for (TopicPartition topicPartition : partitions) {
            topicPartitionWriters.put(topicPartition, newTopicPartitionWriter(topicPartition));
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            try {
                topicPartitionWriters.get(partition).close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private TopicPartitionWriter newTopicPartitionWriter(TopicPartition topicPartition) {
        return new TopicPartitionWriter(
                topicPartition, config, reporter, partitioner, recordWriterProvider
        );
    }

    /**
     * I will generate a random alphanumeric string of given length.
     *
     * @return Generated random alphanumeric string
     */
    //    public String generateRandomAlphanumericString(int length) {
    //        return RandomStringUtils.randomAlphanumeric(length);
    //    }

    private Partitioner getPartitioner() {
        PartitionStrategy strategy = PartitionStrategy.valueOf(config.getPartitionStrategy());
        logger.info("Partition strategy configured: {}", strategy);

        switch (strategy) {
            case TIME: return new TimePartitioner(config);
            case FIELD: return new FieldPartitioner(config);
            default: return new DefaultPartitioner(config);
        }
    }
}
