package io.coffeebeans.connector.sink;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class name is provided to connect runtime over REST API
 * for configuration -> 'connector.class' , This class is called
 * used by connect runtime internally to perform any operation
 * during starting or stopping the connector. This class also
 * configures the list of configs based on the configuration
 * {@code 'tasks.max'} .
 *
 * <p>A connect-runtime can have multiple connectors running.
 */
public class AzureBlobSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(SinkConnector.class);

    private Map<String, String> configProps;

    /**
     * Called by connect-runtime before configuring and starting
     * the sink task. Map of configuration properties is passed as
     * argument. This map of configuration properties need to be used by
     * {@link #taskConfigs(int) taskConfigs(maxTasks)} to configure
     * multiple sink tasks.
     *
     * @param props Map of configuration properties passed over REST API to connect-runtime
     */
    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Azure Blob Sink Connector ...................");
        configProps = props;
    }

    /**
     * Called by the connect-runtime to get the Class extending Task.
     * Multiple instances of this class will be created by the
     * connect-runtime based on the configuration {@code 'tasks.max'}
     * to perform sink task.
     *
     * @return Class extending the Task class implementing the
     *         logic to perform the sink task
     */
    @Override
    public Class<? extends Task> taskClass() {
        return AzureBlobSinkTask.class;
    }

    /**
     * Called by the connect-runtime to get the list of map of
     * configuration properties received by the {@link #start(Map) start(Map)}
     * method. The length of the list should be equal to the maxTasks.
     *
     * @param maxTasks Configuration property passed over
     *                 REST API {@code 'tasks.max'}
     * @return List of Map of configuration properties passed
     *         to the connect-runtime over REST API. The length of the
     *         list is equal to the number of the tasks to be configured
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Maximum tasks to be configured: {}", maxTasks);

        final List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(configProps);
        }
        return configs;
    }

    /**
     * Called by the connect-runtime before stopping the connector.
     */
    @Override
    public void stop() {
        log.info("Stopping Azure Blob Sink Connector ...................");
    }

    /**
     * Called by the connect-runtime to get the ConfigDef.
     *
     * @return ConfigDef
     */
    @Override
    public ConfigDef config() {
        return AzureBlobSinkConfig.getConfig();
    }

    /**
     * Called by the connect-runtime to get the current version of the connector.
     *
     * @return Version of the connector.
     */
    @Override
    public String version() {
        return Version.getVersion();
    }
}
