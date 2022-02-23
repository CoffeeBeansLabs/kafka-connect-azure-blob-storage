package io.coffeebeans.connector.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.storage.AzureBlobStorageManager;
import io.coffeebeans.connector.sink.util.StructToMap;
import io.coffeebeans.connector.sink.util.Version;
import org.apache.kafka.connect.data.Struct;
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

        // Loop through each record and store it in the blob storage.
        for (SinkRecord record : records) {
            logger.info("Task record value: " + record.value());

            Map<?, ?> valueMap;

            // Only Map or Struct type objects are supported currently
            // Check the type of record and get the Map representation
            if (record.value() instanceof Map) {
                valueMap = (Map<?, ?>) record.value();

            } else if (record.value() instanceof Struct){
                // Convert Struct to Map
                valueMap = StructToMap.toJsonMap((Struct) record.value());

            } else return;


            // Check if record is empty; if empty then return;
            if (valueMap.isEmpty()) return;

            // Get the blob name using the identifier key
            String blobName = (String) valueMap.get(blobIdentifierKey);

            byte[] data;
            try {
                // Get bytes array of the value map and persist it in the blob storage
                data = getValueAsBytes(valueMap);
                this.storageManager.upload(containerName, blobName, data);

            } catch (Exception e) {
                logger.error("Unable to process record");
                logger.error(e.toString());
            }
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping Sink Task ...................");
    }

    private byte[] getValueAsBytes(Map<?, ?> valueMap) throws JsonProcessingException {
        return objectMapper.writeValueAsBytes(valueMap);
    }

}
