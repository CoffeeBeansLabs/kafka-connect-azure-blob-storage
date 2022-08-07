package io.coffeebeans.connector.sink;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.exception.SchemaNotFoundException;
import io.coffeebeans.connector.sink.format.FileFormat;
import io.coffeebeans.connector.sink.format.RecordWriterProvider;
import io.coffeebeans.connector.sink.format.SchemaStore;
import io.coffeebeans.connector.sink.format.avro.AvroRecordWriterProvider;
import io.coffeebeans.connector.sink.format.avro.AvroSchemaStore;
import io.coffeebeans.connector.sink.format.json.JsonRecordWriterProvider;
import io.coffeebeans.connector.sink.format.parquet.ParquetRecordWriterProvider;
import io.coffeebeans.connector.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import io.coffeebeans.connector.sink.partitioner.Partitioner;
import io.coffeebeans.connector.sink.partitioner.field.FieldPartitioner;
import io.coffeebeans.connector.sink.partitioner.time.TimePartitioner;
import io.coffeebeans.connector.sink.storage.AzureBlobStorageManager;
import io.coffeebeans.connector.sink.storage.StorageManager;
import io.coffeebeans.connector.sink.util.Version;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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

    private AzureBlobSinkConfig config;
    private SchemaStore schemaStore;
    private SinkTaskContext sinkTaskContext;
    private AzureBlobSinkConnectorContext azureBlobSinkConnectorContext;
    private Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;

    /**
     * Get the current version of the connector. Used by the connector cluster to
     * know which node is running which version when running in distributed mode.
     * The version can be changed in the application.properties file.
     *
     * @return current version
     */
    @Override
    public String version() {
        return Version.getVersion();
    }

    /**
     * Invoked by the connect-runtime to start the sink task.
     *
     * @param configProps map of config props
     */
    @Override
    public void start(Map<String, String> configProps) {
        log.info("Starting Sink Task ....................");

        config = new AzureBlobSinkConfig(configProps);
        schemaStore = getSchemaStore(config.getFileFormat());

        Partitioner partitioner = getPartitioner(config.getPartitionStrategy());
        RecordWriterProvider recordWriterProvider = getRecordWriterProvider(config.getFileFormat());

        StorageManager storageManager = getStorage(
                config.getConnectionString(),
                config.getContainerName()
        );

        try {
            this.azureBlobSinkConnectorContext = AzureBlobSinkConnectorContext.builder(configProps)
                    .withParsedConfig(config)
                    .withSinkTaskContext(sinkTaskContext)
                    .withStorage(storageManager)
                    .withSchemaStore(schemaStore)
                    .withPartitioner(partitioner)
                    .withRecordWriterProvider(recordWriterProvider)
                    .build();

        } catch (IOException | SchemaNotFoundException e) {
            log.error("Failed to start the connector: Error loading schema");
            throw new RuntimeException("Failed to start connector");
        }

        topicPartitionWriters = new HashMap<>();
        open(sinkTaskContext.assignment());

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

        long startTime = System.currentTimeMillis();

        // Loop through each record and store it in the buffer.
        for (SinkRecord record : records) {
            if (record.value() == null) {
                continue;
            }
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
            TopicPartitionWriter topicPartitionWriter = topicPartitionWriters.get(topicPartition);

            if (topicPartitionWriter == null) {
                topicPartitionWriter = newTopicPartitionWriter();
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

        log.info("Processed {} records in {} ms time", collection.size(), System.currentTimeMillis() - startTime);
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
        this.sinkTaskContext = context;
    }

    /**
     * Invoked after starting the task to open any resources.
     *
     * @param topicPartitions collection of topic partitions
     */
    @Override
    public void open(Collection<TopicPartition> topicPartitions) {
        for (TopicPartition topicPartition : topicPartitions) {
            topicPartitionWriters.put(topicPartition, newTopicPartitionWriter());
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

        for (TopicPartition topicPartition : topicPartitionWriters.keySet()) {
            Long offsetToCommit = topicPartitionWriters.get(topicPartition)
                    .getLastSuccessfulOffset();

            if (offsetToCommit == null) {
                continue;
            }
            offsetsToCommit.put(topicPartition, new OffsetAndMetadata(offsetToCommit));
        }
        return offsetsToCommit;
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
     * Returns a new instance of TopicPartitionWriter.
     *
     * @return new instance of TopicPartitionWriter
     */
    private TopicPartitionWriter newTopicPartitionWriter() {

        return new TopicPartitionWriter(this.azureBlobSinkConnectorContext);
    }

    /**
     * Returns a new instance of Partitioner based on the provided partition strategy.
     *
     * @return Partitioner
     */
    public Partitioner getPartitioner(String partitionStrategy) {
        PartitionStrategy strategy = PartitionStrategy.valueOf(partitionStrategy);
        log.info("Partition strategy configured: {}", strategy);

        switch (strategy) {
            case TIME: return new TimePartitioner(config);
            case FIELD: return new FieldPartitioner(config);
            default: return new DefaultPartitioner(config);
        }
    }

    /**
     * Returns appropriate RecordWriterProvider based on provided file format.
     *
     * @param fileFormat User configured file format e.g. PARQUET
     * @return RecordWriterProvider
     */
    public RecordWriterProvider getRecordWriterProvider(String fileFormat) {
        FileFormat format = FileFormat.valueOf(fileFormat);

        switch (format) {
            case PARQUET: return getParquetRecordWriterProvider(fileFormat);
            case AVRO: return getAvroRecordWriterProvider(fileFormat);
            case JSON: return getJsonRecordWriterProvider(fileFormat);
            default: return null;
        }
    }

    /**
     * Returns appropriate schema store based on provided file format.
     *
     * @param fileFormat User configured file format e.g. PARQUET
     * @return SchemaStore
     */
    public SchemaStore getSchemaStore(String fileFormat) {
        if (this.schemaStore != null) {
            return this.schemaStore;
        }

        FileFormat format = FileFormat.valueOf(fileFormat);

        switch (format) {
            case PARQUET:
            case AVRO:
            case JSON: {
                this.schemaStore = AvroSchemaStore.getSchemaStore();
                return this.schemaStore;
            }
            default: return null;
        }
    }

    /**
     * Returns a new instance of ParquetRecordWriterProvider.
     * FileFormat is used to get the schema store.
     *
     * @param fileFormat User configured file format e.g. PARQUET
     * @return RecordWriterProvider
     */
    private ParquetRecordWriterProvider getParquetRecordWriterProvider(String fileFormat) {
        SchemaStore schemaStore = getSchemaStore(fileFormat);

        return new ParquetRecordWriterProvider(schemaStore);
    }

    private AvroRecordWriterProvider getAvroRecordWriterProvider(String fileFormat) {
        SchemaStore schemaStore = getSchemaStore(fileFormat);

        return new AvroRecordWriterProvider(schemaStore);
    }

    private JsonRecordWriterProvider getJsonRecordWriterProvider(String fileFormat) {
        SchemaStore schemaStore = getSchemaStore(fileFormat);

        return new JsonRecordWriterProvider(schemaStore);
    }

    /**
     * Returns new AzureBlobStorage instance.
     *
     * @param connectionString Valid connection string for Azure blob storage
     * @param containerName Container name where blobs will be stored
     * @return Storage instance
     */
    public StorageManager getStorage(String connectionString, String containerName) {
        return new AzureBlobStorageManager(connectionString, containerName);
    }
}
