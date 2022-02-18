package io.coffeebeans.connector.sink;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.storage.AzureBlobStorageManager;
import io.coffeebeans.connector.sink.util.Version;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class AzureBlobSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(AzureBlobSinkTask.class);
    private AzureBlobSinkConfig config;
    private AzureBlobStorageManager storageManager;
    private String containerName;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> config) {
        logger.info("Starting Sink Task ....................");
        this.config = new AzureBlobSinkConfig(config);

        String connectionString = this.config.getConnectionString();
        this.containerName = this.config.getContainerName();
        logger.info("Connection String: {}", connectionString);
        logger.info("Container Name: {}", this.containerName);
        this.storageManager = new AzureBlobStorageManager(connectionString);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        for (SinkRecord record : collection) {
            logger.info("Task record value: " + record.value());
            this.storageManager.upload(this.containerName, record.value());
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping Sink Task ...................");
    }
}
