package io.coffeebeans.connect.azure.blob.sink;

import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connect.azure.blob.sink.config.NullValueBehavior;
import io.coffeebeans.connect.azure.blob.sink.format.Format;
import io.coffeebeans.connect.azure.blob.sink.format.RecordWriterProvider;
import io.coffeebeans.connect.azure.blob.sink.format.SchemaStore;
import io.coffeebeans.connect.azure.blob.sink.format.avro.AvroRecordWriterProvider;
import io.coffeebeans.connect.azure.blob.sink.format.avro.AvroSchemaStore;
import io.coffeebeans.connect.azure.blob.sink.format.bytearray.ByteArrayRecordWriterProvider;
import io.coffeebeans.connect.azure.blob.sink.format.json.JsonRecordWriterProvider;
import io.coffeebeans.connect.azure.blob.sink.format.parquet.ParquetRecordWriterProvider;
import io.coffeebeans.connect.azure.blob.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connect.azure.blob.sink.partitioner.PartitionStrategy;
import io.coffeebeans.connect.azure.blob.sink.partitioner.Partitioner;
import io.coffeebeans.connect.azure.blob.sink.partitioner.field.FieldPartitioner;
import io.coffeebeans.connect.azure.blob.sink.partitioner.time.TimePartitioner;
import io.coffeebeans.connect.azure.blob.sink.storage.AzureBlobStorageManager;
import io.coffeebeans.connect.azure.blob.sink.storage.StorageManager;
import io.coffeebeans.connect.azure.blob.util.Version;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
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

    private SchemaStore schemaStore;
    private boolean ignoreNullValues;
    private AzureBlobSinkConfig config;
    private StorageManager storageManager;
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
        schemaStore = getSchemaStore(config.getFormat());
        this.storageManager = getStorage(
                config.getConnectionString(),
                config.getContainerName()
        );
        storageManager.configure(getRetryConfigMap(config));

        Partitioner partitioner = getPartitioner(config.getPartitionStrategy());

        RecordWriterProvider recordWriterProvider = getRecordWriterProvider(config.getFormat());
        recordWriterProvider.configure(config);

        this.azureBlobSinkConnectorContext = AzureBlobSinkConnectorContext.builder(configProps)
                .withParsedConfig(config)
                .withSinkTaskContext(sinkTaskContext)
                .withStorage(storageManager)
                .withSchemaStore(schemaStore)
                .withPartitioner(partitioner)
                .withRecordWriterProvider(recordWriterProvider)
                .build();

        topicPartitionWriters = new HashMap<>();

        String nullValueBehavior = config.getNullValueBehavior();
        ignoreNullValues = NullValueBehavior.IGNORE.toString()
                .equals(nullValueBehavior);

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
        log.trace("Received {} records", collection.size());

        long startTime = System.currentTimeMillis();

        // Loop through each record and store it in the buffer.
        for (SinkRecord record : collection) {
            if (record.value() == null) {
                handleNullValues();
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

            } catch (ConnectException e) {
                throw e;

            } catch (Exception e) {
                log.trace("Failed to process record with exception: {}", e.getMessage());
            }
        }

        log.trace("Processed {} records in {} ms time", collection.size(), System.currentTimeMillis() - startTime);
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
            topicPartitionWriters.put(topicPartition, newTopicPartitionWriter(topicPartition));
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

            } catch (Exception e) {
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
    private TopicPartitionWriter newTopicPartitionWriter(TopicPartition topicPartition) {

        return new TopicPartitionWriter(topicPartition, this.azureBlobSinkConnectorContext);
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
     * @param formatString User configured file format e.g. PARQUET
     * @return RecordWriterProvider
     */
    public RecordWriterProvider getRecordWriterProvider(String formatString) {
        Format format = Format.valueOf(formatString);

        switch (format) {
            case PARQUET: return new ParquetRecordWriterProvider(storageManager, schemaStore);
            case AVRO: return new AvroRecordWriterProvider(storageManager, schemaStore);
            case JSON: return new JsonRecordWriterProvider(storageManager);
            case BYTEARRAY: return new ByteArrayRecordWriterProvider(storageManager);
            default: throw new ConnectException("Invalid format: " + formatString);
        }
    }

    /**
     * Returns appropriate schema store based on provided file format.
     *
     * @param formatString User configured file format e.g. PARQUET
     * @return SchemaStore
     */
    public SchemaStore getSchemaStore(String formatString) {
        Format format = Format.valueOf(formatString);

        switch (format) {
            case PARQUET:
            case AVRO:
            case JSON:
            case BYTEARRAY:
                return AvroSchemaStore.getSchemaStore();
            default: throw new ConnectException("Invalid format: " + formatString);
        }
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

    private void handleNullValues() {
        if (ignoreNullValues) {
            return;
        }
        throw new ConnectException("Received null value, stopping the execution");
    }

    private Map<String, Object> getRetryConfigMap(AzureBlobSinkConfig configProp) {

        Map<String, Object> configMap = new HashMap<>();

        configMap.put(AzureBlobSinkConfig.RETRY_TYPE_CONF, configProp.getRetryType());
        configMap.put(AzureBlobSinkConfig.RETRIES_CONF, configProp.getMaxRetries());
        configMap.put(AzureBlobSinkConfig.CONNECTION_TIMEOUT_MS_CONF, configProp.getConnectionTimeoutMs());
        configMap.put(AzureBlobSinkConfig.RETRY_BACKOFF_MS_CONF, configProp.getRetryBackoffMs());
        configMap.put(AzureBlobSinkConfig.RETRY_MAX_BACKOFF_MS_CONF, configProp.getRetryMaxBackoffMs());

        return configMap;
    }
}
