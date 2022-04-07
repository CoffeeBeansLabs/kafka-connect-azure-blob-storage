package io.coffeebeans.connector.sink;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.util.Version;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will be initialized by the SinkConnector class and will be passed with configuration
 * props.
 */
public class AzureBlobSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(SinkTask.class);

    private RecordWriter recordWriter;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> configProps) {
        logger.info("Starting Sink Task ....................");
        AzureBlobSinkConfig config = new AzureBlobSinkConfig(configProps);

        this.recordWriter = new RecordWriter(config);
        this.recordWriter.startMetadataConsumer();
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.isEmpty()) {
            return;
        }

        List<SinkRecord> records = new ArrayList<>(collection);
        logger.info("Received {} records", records.size());

        // Loop through each record and store it in the blob storage.
        for (SinkRecord record : records) {

            try {
                this.recordWriter.bufferRecord(record);

            } catch (Exception e) {
                logger.error("Failed to process record with offset: {}", record.kafkaOffset());
            }
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping Sink Task ...................");
    }
}
