package io.coffeebeans.connector.sink;

import static io.coffeebeans.connector.sink.config.AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY;
import static io.coffeebeans.connector.sink.config.AzureBlobSinkConfig.TOPIC_SCHEMA_URL_SUFFIX;
import static org.apache.kafka.connect.sink.SinkTask.TOPICS_CONFIG;

import com.google.common.base.Splitter;
import io.coffeebeans.connector.sink.exception.SchemaNotFoundException;
import io.coffeebeans.connector.sink.format.FileFormat;
import io.coffeebeans.connector.sink.format.SchemaStore;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This context class is different from ConnectorContext & SinkTaskContext.
 * Its usability is inside the scope of AzureBlobSinkConnector.
 */
public class AzureBlobSinkConnectorContext {
    private final Logger log = LoggerFactory.getLogger(AzureBlobSinkConnectorContext.class);

    private SchemaStore schemaStore;
    private Map<String, String> configProps;

    /**
     * Private constructor to be used by builder class.
     * Builder classes make testing simpler.
     *
     * @param builder Builder object
     * @throws IOException IOException
     * @throws SchemaNotFoundException SchemaNotFoundException
     */
    private AzureBlobSinkConnectorContext(Builder builder) throws
            IOException, SchemaNotFoundException {

        this.configProps = builder.configProps;
        this.schemaStore = builder.schemaStore;

        if (isSchemaStoreRecommended(configProps)) {
            try {
                loadSchema(configProps, schemaStore);

            } catch (IOException | SchemaNotFoundException e) {
                log.error("Failed to initialize context: Error loading schema");
                throw e;
            }
        }
    }

    /**
     * Check if using schema store recommended.
     * It will check the file format specified by the user. As of now we are
     * only using schema store for writing parquet files, so it will recommend
     * using schema store only if the file format is parquet.
     *
     * @param configProps Configuration map
     * @return True if schema store usage is recommended or else false
     */
    private boolean isSchemaStoreRecommended(Map<String, String> configProps) {
        FileFormat fileFormat = FileFormat.valueOf(
                configProps.get(FILE_FORMAT_CONF_KEY)
        );
        return FileFormat.PARQUET.equals(fileFormat);
    }

    /**
     * Get the user configured list of topics, split it and get the configured
     * schema url for that topic.
     *
     * <p>Register that schema with the schema store.
     *
     * @param schemaStore Schema store where schema will be registered
     */
    private void loadSchema(Map<String, String> configProps, SchemaStore schemaStore)
            throws IOException, SchemaNotFoundException {

        String topics = configProps.get(TOPICS_CONFIG);

        List<String> topicList = Splitter.on(',')
                .trimResults()
                .omitEmptyStrings()
                .splitToList(topics);

        for (String topic : topicList) {
            String schemaURL = getSchemaURL(configProps, topic)
                    .orElseThrow(() -> new SchemaNotFoundException("Schema url not configured for topic: " + topic));

            loadSchema(schemaStore, topic, schemaURL);
        }
    }

    /**
     * Register the schema to the provided SchemaStore.
     *
     * @param schemaStore SchemaStore to store the TopicPartition and it's respective Schema
     * @param topic Topic as key for storing the schema
     * @param schemaURL Schema as the value to be stored in the Schema store
     * @throws IOException Thrown if encounters any error while registering the schema like
     *      malformed URL, stream closed, etc.
     */
    private void loadSchema(SchemaStore schemaStore, String topic, String schemaURL)
            throws IOException {

        schemaStore.register(topic, schemaURL);
    }

    /**
     * Returns the configured schema url for the given topic.
     *
     * @param configProps Map of config provided by the user
     * @param topic kafka topic
     * @return Schema URL for the topic if present or null
     */
    private Optional<String> getSchemaURL(Map<String, String> configProps, String topic) {

        String topicSchemaURLConfig = topic + TOPIC_SCHEMA_URL_SUFFIX;
        return Optional.ofNullable(
                configProps.get(topicSchemaURLConfig)
        );
    }


    /**
     * Builder method.
     *
     * @param configProps Config properties
     * @return Builder
     */
    public static AzureBlobSinkConnectorContext.Builder builder(Map<String, String> configProps) {
        return new Builder(configProps);
    }

    /**
     * Builder class.
     */
    public static class Builder {

        private SchemaStore schemaStore;
        private final Map<String, String> configProps;

        private Builder(Map<String, String> configProps) {
            this.configProps = configProps;
        }

        public Builder schemaStore(SchemaStore schemaStore) {
            this.schemaStore = schemaStore;
            return this;
        }

        public AzureBlobSinkConnectorContext build() throws IOException, SchemaNotFoundException {
            return new AzureBlobSinkConnectorContext(this);
        }
    }
}
