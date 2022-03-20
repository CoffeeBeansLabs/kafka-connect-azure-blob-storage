package io.coffeebeans.connector.sink;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.util.Version;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will be the first to run when the connector is configured.
 */
public class AzureBlobSinkConnector extends SinkConnector {
    private static final Logger logger = LoggerFactory.getLogger(AzureBlobSinkConnector.class);
    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting Azure Blob Sink Connector ...................");
        this.configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AzureBlobSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logger.info("Maximum tasks to be configured: {}", maxTasks);

        final List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void stop() {
        logger.info("Stopping Azure Blob Sink Connector ...................");
    }

    @Override
    public ConfigDef config() {
        return AzureBlobSinkConfig.getConfig();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
