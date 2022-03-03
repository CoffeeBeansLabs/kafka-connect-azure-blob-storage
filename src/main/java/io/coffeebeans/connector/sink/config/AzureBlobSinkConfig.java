package io.coffeebeans.connector.sink.config;

import io.coffeebeans.connector.sink.config.validators.ConnectionStringValidator;
import io.coffeebeans.connector.sink.config.validators.ContainerNameValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Validator;

import java.util.Map;

public class AzureBlobSinkConfig extends AbstractConfig {

    // Connection related configurations
    public static final String CONN_STRING_CONF = "connection.string";
    public static final Validator CONN_STRING_VALIDATOR = new ConnectionStringValidator();
    public static final String CONN_STRING_DOC = "Connection string of the azure blob storage";

    // Container related configurations
    public static final String CONTAINER_NAME_CONF = "container.name";
    public static final String CONTAINER_NAME_DEFAULT = "default";
    public static final Validator CONTAINER_NAME_VALIDATOR = new ContainerNameValidator();
    public static final String CONTAINER_NAME_DOC = "Container name to store the blobs";

    // Blob related configurations
    public static final String BLOB_IDENTIFIER_KEY = "blob.identifier.key";
    public static final String BLOB_IDENTIFIER_KEY_DEFAULT = "blob";
    public static final String BLOB_IDENTIFIER_KEY_DOC = "Key to identify blob";

    // Partitioner related configurations
    public static final String TOPIC_DIR = "topic.dir";
    public static final String TOPIC_DIR_DOC = "Parent directory where data from this topic will be stored";

    public static final String PARTITION_STRATEGY_CONF = "partition.strategy";
    public static final String PARTITION_STRATEGY_DEFAULT = "DEFAULT";
    public static final String PARTITION_STRATEGY_DOC = "Partition strategy to be used";

    // Field based partitioning configurations
    public static final String PARTITION_STRATEGY_FIELD_CONF = "partition.strategy.field";
    public static final String PARTITION_STRATEGY_FIELD_DEFAULT = null;
    public static final String PARTITION_STRATEGY_FIELD_DOC = "Field name for field-based partitioning";

    // Common validators
    public static final Validator NON_EMPTY_STRING_VALIDATOR = new ConfigDef.NonEmptyString();


    // properties
    private final String connectionString;
    private final String containerName;
    private final String blobIdentifier;
    private final String topicDir;
    private final String partitionStrategy;
    private final String fieldName;

    public AzureBlobSinkConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);
    }

    public AzureBlobSinkConfig(ConfigDef configDef, Map<String, String> parsedConfig) {
        super(configDef, parsedConfig);
        this.connectionString = this.getString(CONN_STRING_CONF);
        this.containerName = this.getString(CONTAINER_NAME_CONF);
        this.blobIdentifier = this.getString(BLOB_IDENTIFIER_KEY);
        this.topicDir = this.getString(TOPIC_DIR);
        this.partitionStrategy = this.getString(PARTITION_STRATEGY_CONF);
        this.fieldName = this.getString(PARTITION_STRATEGY_FIELD_CONF);
    }


    public static ConfigDef getConfig() {
        ConfigDef configDef = new ConfigDef();
        defineConfig(configDef);

        return configDef;
    }

    public static void defineConfig(ConfigDef configDef) {
        configDef
                .define(
                        CONN_STRING_CONF,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        CONN_STRING_VALIDATOR,
                        ConfigDef.Importance.HIGH,
                        CONN_STRING_DOC
                )
                .define(
                        CONTAINER_NAME_CONF,
                        ConfigDef.Type.STRING,
                        CONTAINER_NAME_DEFAULT,
                        CONTAINER_NAME_VALIDATOR,
                        ConfigDef.Importance.LOW,
                        CONTAINER_NAME_DOC
                )
                .define(
                        BLOB_IDENTIFIER_KEY,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.LOW,
                        BLOB_IDENTIFIER_KEY_DOC
                )
                .define( // Name of directory to store the records
                        TOPIC_DIR,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        NON_EMPTY_STRING_VALIDATOR,
                        ConfigDef.Importance.MEDIUM,
                        TOPIC_DIR_DOC
                )
                .define( // Partition strategy configuration
                        PARTITION_STRATEGY_CONF,
                        ConfigDef.Type.STRING,
                        PARTITION_STRATEGY_DEFAULT,
                        NON_EMPTY_STRING_VALIDATOR,
                        ConfigDef.Importance.MEDIUM,
                        PARTITION_STRATEGY_DOC
                )
                .define( // Field-based partition strategy configuration
                        PARTITION_STRATEGY_FIELD_CONF,
                        ConfigDef.Type.STRING,
                        PARTITION_STRATEGY_FIELD_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        PARTITION_STRATEGY_FIELD_DOC
                );
    }

    public String getConnectionString() {
        return this.connectionString;
    }

    public String getContainerName() {
        return this.containerName;
    }

    public String getBlobIdentifier() {
        return this.blobIdentifier;
    }

    public String getTopicDir() {
        return this.topicDir;
    }

    public String getPartitionStrategy() {
        return this.partitionStrategy;
    }

    public String getFieldName() {
        return this.fieldName;
    }
}
