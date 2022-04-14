package io.coffeebeans.connector.sink;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.util.Version;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
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
    private static final Logger logger = LoggerFactory.getLogger(SinkTask.class);
    public static String UNIQUE_TASK_IDENTIFIER;

    private RecordWriter recordWriter;
    private SinkTaskContext context;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> configProps) {
        logger.info("Starting Sink Task ....................");
        UNIQUE_TASK_IDENTIFIER = generateRandomAlphanumericString(4);
        logger.info("Unique string: " + UNIQUE_TASK_IDENTIFIER);

        AzureBlobSinkConfig config = new AzureBlobSinkConfig(configProps);
        this.recordWriter = new RecordWriter(config);

        logger.info("Sink Task started successfully ....................");
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
;        if (collection.isEmpty()) {
            return;
        }

        List<SinkRecord> records = new ArrayList<>(collection);
        logger.info("Received {} records", records.size());

        long startTime = System.currentTimeMillis();
        // Loop through each record and store it in the blob storage.
        for (SinkRecord record : records) {

            try {
                this.recordWriter.bufferRecord(record);

            } catch (Exception e) {
                logger.error("Failed to process record with offset: {}", record.kafkaOffset());
                logger.error(e.getMessage());
            }
        }
        logger.info("Processed {} records in {} milli seconds", records.size(), System.currentTimeMillis() - startTime);
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

    /**
     * I will generate a random alphanumeric string of given length.
     *
     * @param length Length of the required alphanumeric string
     * @return Generated random alphanumeric string
     */
    public String generateRandomAlphanumericString(int length) {
        return RandomStringUtils.randomAlphanumeric(length);
    }
}
