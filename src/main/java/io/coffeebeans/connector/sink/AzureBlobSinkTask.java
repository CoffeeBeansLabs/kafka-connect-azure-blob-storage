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

import java.util.*;

public class AzureBlobSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(AzureBlobSinkTask.class);

    private String containerName;
    private String blobIdentifierKey;
    private ObjectMapper objectMapper;
    private AzureBlobStorageManager storageManager;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> configProps) {
        logger.info("Starting Sink Task ....................");
        AzureBlobSinkConfig config = new AzureBlobSinkConfig(configProps);

        containerName = config.getContainerName();
        blobIdentifierKey = config.getBlobIdentifier();
        objectMapper = new ObjectMapper();
        storageManager = new AzureBlobStorageManager(config.getConnectionUrl());

        logger.debug("Blob identifier key: {}", this.blobIdentifierKey);
        logger.debug("Container name: {}", this.containerName);
        logger.debug("Connection url: {}", config.getConnectionUrl());
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        List<SinkRecord> records = new ArrayList<>(collection);
        logger.debug("Received {} records", records.size());

        for (SinkRecord record : records) {
            logger.info("Task record value: " + record.value());

            // Processing only JSON messages
            if (record.value() instanceof Map) {
                byte[] data;

                Map<?, ?> value = (Map<?, ?>) record.value();

                // Check if the JSON has no keys
                if (value.isEmpty()) {
                    return;
                }
                String blobName = (String) value.get(blobIdentifierKey);

                try {
                    // Converting Map to byte array
                    data = objectMapper.writeValueAsBytes(record.value());
                    this.storageManager.upload(this.containerName, blobName, data);
                } catch (JsonProcessingException e) {
                    logger.error("Error processing JSON value");
                    logger.error(e.toString());
                }
            }
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping Sink Task ...................");
    }

}
