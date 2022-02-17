package io.coffeebeans.connector.sink;

import io.coffeebeans.connector.sink.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AzureBlobSinkConnector extends SinkConnector {
    private static final Logger logger = LoggerFactory.getLogger(AzureBlobSinkConnector.class);

    @Override
    public void start(Map<String, String> map) {
        logger.info("Starting Sink Connector ...................");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AzureBlobSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        configs.add(new HashMap<>());
        return configs;
    }

    @Override
    public void stop() {
        logger.info("Stopping Sink Connector ...................");
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
