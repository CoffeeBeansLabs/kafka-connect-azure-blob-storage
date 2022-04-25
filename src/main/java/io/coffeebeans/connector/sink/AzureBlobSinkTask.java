package io.coffeebeans.connector.sink;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.RecordWriterProvider;
import io.coffeebeans.connector.sink.format.parquet.ParquetRecordWriterProvider;
import io.coffeebeans.connector.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import io.coffeebeans.connector.sink.partitioner.Partitioner;
import io.coffeebeans.connector.sink.partitioner.field.FieldPartitioner;
import io.coffeebeans.connector.sink.partitioner.time.TimePartitioner;
import io.coffeebeans.connector.sink.storage.StorageFactory;
import io.coffeebeans.connector.sink.util.Version;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private static final Logger log = LoggerFactory.getLogger(AzureBlobSinkTask.class);

    private SinkTaskContext context;
    private Partitioner partitioner;
    private AzureBlobSinkConfig config;
    private ErrantRecordReporter reporter;
    private RecordWriterProvider recordWriterProvider;
    private Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;

    /**
     * Current version of the connector.
     *
     * @return current version
     */
    @Override
    public String version() {
        return Version.getVersion();
    }

    /**
     * Invoked by the connect-runtime to start the task.
     *
     * @param configProps map of config props
     */
    @Override
    public void start(Map<String, String> configProps) {
        log.info("Starting Sink Task ....................");

        config = new AzureBlobSinkConfig(configProps);
        topicPartitionWriters = new HashMap<>();
        partitioner = getPartitioner();

        StorageFactory.set(config.getConnectionString(), config.getContainerName());
        reporter = context.errantRecordReporter();
        recordWriterProvider = new ParquetRecordWriterProvider();
        open(context.assignment());

        log.info("Sink Task started successfully ....................");
    }

    /**
     * Invoked by the connect-runtime to put the records in to sink.
     * It receives a collection of sink records.
     *
     * @param collection collection of sink records
     */
    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.isEmpty()) {
            return;
        }
        List<SinkRecord> records = new ArrayList<>(collection);
        log.info("Received {} records", records.size());

        long startTime = System.nanoTime();

        // Loop through each record and store it in the buffer.
        for (SinkRecord record : records) {
            if (record.value() == null) {
                continue;
            }
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
            TopicPartitionWriter topicPartitionWriter = topicPartitionWriters.get(topicPartition);

            if (topicPartitionWriter == null) {
                topicPartitionWriter = newTopicPartitionWriter(topicPartition);
                topicPartitionWriters.put(topicPartition, topicPartitionWriter);
            }
            topicPartitionWriter.buffer(record);
        }

        for (TopicPartitionWriter writer : topicPartitionWriters.values()) {
            try {
                writer.write();
            } catch (Exception e) {
                log.error("Failed to process record with exception: {}", e.getMessage());
            }
        }

        log.info("Processed {} records in {} ns time", collection.size(), System.nanoTime() - startTime);
    }

    /**
     * Invoked when stopping the task.
     */
    @Override
    public void stop() {
        log.info("Stopping Sink Task ...................");
    }

    /**
     * Initialize the SinkContext.
     *
     * @param context Sink context
     */
    @Override
    public void initialize(SinkTaskContext context) {
        this.context = context;
    }

    /**
     * Invoked after starting the task to open any resources.
     *
     * @param topicPartitions collection of topic partitions
     */
    @Override
    public void open(Collection<TopicPartition> topicPartitions) {
        for (TopicPartition topicPartition : topicPartitions) {
            topicPartitionWriters.put(topicPartition, newTopicPartitionWriter(topicPartition));
        }
    }

    /**
     * Invoked before stopping the task to close any resource.
     *
     * @param topicPartitions Collection of topic partition
     */
    @Override
    public void close(Collection<TopicPartition> topicPartitions) {
        for (TopicPartition topicPartition : topicPartitions) {
            try {
                topicPartitionWriters.get(topicPartition).close();
            } catch (IOException e) {
                log.error("Failed to close TopicPartition, topic: {}, partition: {}",
                        topicPartition.topic(), topicPartition.partition()
                );
            }
        }
    }

    /**
     * Get a new instance of TopicPartitionWriter.
     *
     * @param topicPartition Topic Partition
     * @return TopicPartitionWriter
     */
    private TopicPartitionWriter newTopicPartitionWriter(TopicPartition topicPartition) {
        return new TopicPartitionWriter(
                topicPartition, config, reporter, partitioner, recordWriterProvider
        );
    }

    /**
     * Initialize partitioner based on the strategy configured.
     *
     * @return Partitioner
     */
    private Partitioner getPartitioner() {
        PartitionStrategy strategy = PartitionStrategy.valueOf(config.getPartitionStrategy());
        log.info("Partition strategy configured: {}", strategy);

        switch (strategy) {
          case TIME: return new TimePartitioner(config);
          case FIELD: return new FieldPartitioner(config);
          default: return new DefaultPartitioner(config);
        }
    }
}
