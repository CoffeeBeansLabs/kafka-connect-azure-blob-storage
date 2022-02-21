package io.coffeebeans.connector.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private String blobIdentifierKey;
    private ObjectMapper objectMapper;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> config) {
        logger.info("Starting Sink Task ....................");
        this.config = new AzureBlobSinkConfig(config);

        containerName = this.config.getContainerName();
        blobIdentifierKey = this.config.getBlobIdentifier();
        objectMapper = new ObjectMapper();

        String connectionString = this.config.getConnectionString();
        storageManager = new AzureBlobStorageManager(connectionString);
//        logger.info("Connection String: {}", connectionString);
//        logger.info("Container Name: {}", this.containerName);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        for (SinkRecord record : collection) {
            logger.info("Task record value: " + record.value());

            if (record.value() instanceof Map) {
                try {
                    Map<?, ?> values = (Map<?, ?>) record.value();
                    String blobName = (String) values.get(blobIdentifierKey);

                    byte[] data = objectMapper.writeValueAsBytes(record.value());
                    this.storageManager.upload(this.containerName, blobName, data);
                } catch (JsonProcessingException e) {
                    logger.error("Error storing values. " + e);
                }
            }
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping Sink Task ...................");
    }
}
