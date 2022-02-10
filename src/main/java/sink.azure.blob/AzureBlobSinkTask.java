package sink.azure.blob;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class AzureBlobSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(AzureBlobSinkTask.class);

    public AzureBlobSinkTask() {}

    @Override
    public String version() {
        return "1";
    }

    @Override
    public void start(Map<String, String> map) {
        logger.info("Starting Sink Task ....................");
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        for (SinkRecord records : collection) {
            logger.info("Task record value: " + records.value());
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping Sink Task ...................");
    }
}
