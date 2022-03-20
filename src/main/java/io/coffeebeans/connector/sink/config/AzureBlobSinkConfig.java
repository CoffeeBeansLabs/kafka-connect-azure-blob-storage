package io.coffeebeans.connector.sink.config;

import io.coffeebeans.connector.sink.config.recommenders.PartitionStrategyFieldNameRecommender;
import io.coffeebeans.connector.sink.config.recommenders.PartitionStrategyRecommender;
import io.coffeebeans.connector.sink.config.validators.ConnectionStringValidator;
import io.coffeebeans.connector.sink.config.validators.ContainerNameValidator;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Validator;

import static org.apache.kafka.common.config.ConfigDef.Importance;
import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;
import static org.apache.kafka.common.config.ConfigDef.Type;


public class AzureBlobSinkConfig extends AbstractConfig {

    private static final Type TYPE_STRING = Type.STRING;
    private static final Importance IMPORTANCE_LOW = Importance.LOW;
    private static final Importance IMPORTANCE_MEDIUM = Importance.MEDIUM;
    private static final Importance IMPORTANCE_HIGH = Importance.HIGH;


    /**
     * Azure Blob Connection related configurations
     */
    public static final String CONN_URL_CONF_KEY = "connection.url";
    public static final Validator CONN_URL_VALIDATOR = new ConnectionStringValidator();
    public static final String CONN_URL_CONF_DOC = "Connection url of the azure blob storage";

    /**
     * Container configurations where blobs will be stored.
     * If no value provided, a container with name 'default' will be created.
     * ContainerNameValidator checks if the provided value is not null, empty and blank.
     */
    public static final String CONTAINER_NAME_CONF_KEY = "container.name";
    public static final String CONTAINER_NAME_DEFAULT_VALUE = "default";
    public static final Validator CONTAINER_NAME_VALIDATOR = new ContainerNameValidator();
    public static final String CONTAINER_NAME_CONF_DOC = "Name of the container where blobs will be stored";

    /**
     * Parent directory where blobs will be stored.
     */
    // TODO: Configure a default parent directory
    public static final String TOPIC_DIR = "topic.dir";
    public static final String TOPIC_DIR_DOC = "Parent directory where data from this topic will be stored";

    /**
     * Partition strategy configuration.
     */
    // TODO: Have an option for NONE partitioning
    public static final String PARTITION_STRATEGY_CONF = "partition.strategy";
    public static final String PARTITION_STRATEGY_DEFAULT = "DEFAULT";
    public static final Recommender PARTITION_STRATEGY_RECOMMENDER = new PartitionStrategyRecommender();
    public static final String PARTITION_STRATEGY_DOC = "Partition strategy to be used";

    /**
     * Applicable only for field-based partitioning
     */
    // TODO: Add support for List of fields
    public static final String PARTITION_STRATEGY_FIELD_NAME_CONF = "partition.strategy.field.name";
    public static final String PARTITION_STRATEGY_FIELD_NAME_DOC = "Name of the field from which values should be " +
            "extracted";
    public static final Recommender PARTITION_STRATEGY_FIELD_NAME_RECOMMENDER =
            new PartitionStrategyFieldNameRecommender();

    // Time based partitioning configurations
    /**
     * Applicable only for time-based partitioning
     */

    public static final String PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF = "path.format";
    public static final String PARTITION_STRATEGY_TIME_PATH_FORMAT_DOC = "Output file path time partition format";

    public static final String PARTITION_STRATEGY_TIME_BUCKET_MS_CONF = "partition.duration.ms";
    public static final String PARTITION_STRATEGY_TIME_BUCKET_MS_DOC = "Time partition granularity";

    // TODO: Default should be UTC
    public static final String PARTITION_STRATEGY_TIME_TIMEZONE_CONF = "timezone";
    public static final String PARTITION_STRATEGY_TIME_TIMEZONE_DOC = "Timezone for the time partitioner";

    public static final String PARTITION_STRATEGY_TIME_EXTRACTOR_CONF = "timestamp.extractor";
    public static final String PARTITION_STRATEGY_TIME_EXTRACTOR_DOC = "Time extractor for time based partitioner";

    /**
     * Rollover file policy related configurations
     */
    public static final String ROLLOVER_POLICY_SIZE_CONF = "rollover.policy.size";
    public static final String ROLLOVER_POLICY_SIZE_DOC = "Maximum size of the blob for rollover to happen";

    // Common validators
    public static final Validator NON_EMPTY_STRING_VALIDATOR = new ConfigDef.NonEmptyString();


    // properties
    private final String connectionString;
    private final String containerName;
//    private final String blobIdentifier;
    private final String topicDir;
    private final String partitionStrategy;
    private final String fieldName;
    private final String pathFormat;
    private final String timeBucket;
    private final String timezone;
    private final String timeExtractor;
    private final String maxBlobSize;


    public AzureBlobSinkConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);
    }

    public AzureBlobSinkConfig(ConfigDef configDef, Map<String, String> parsedConfig) {
        super(configDef, parsedConfig);
        this.connectionString = this.getString(CONN_URL_CONF_KEY);
        this.containerName = this.getString(CONTAINER_NAME_CONF_KEY);
//        this.blobIdentifier = this.getString(BLOB_IDENTIFIER_KEY);
        this.topicDir = this.getString(TOPIC_DIR);
        this.partitionStrategy = this.getString(PARTITION_STRATEGY_CONF);
        this.fieldName = this.getString(PARTITION_STRATEGY_FIELD_NAME_CONF);
        this.pathFormat = this.getString(PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF);
        this.timeBucket = this.getString(PARTITION_STRATEGY_TIME_BUCKET_MS_CONF);
        this.timezone = this.getString(PARTITION_STRATEGY_TIME_TIMEZONE_CONF);
        this.timeExtractor = this.getString(PARTITION_STRATEGY_TIME_EXTRACTOR_CONF);
        this.maxBlobSize = this.getString(ROLLOVER_POLICY_SIZE_CONF);
    }


    public static ConfigDef getConfig() {
        ConfigDef configDef = new ConfigDef();
        defineConfig(configDef);

        return configDef;
    }

    public static void defineConfig(ConfigDef configDef) {
        configDef
                .define( // MANDATORY
                        CONN_URL_CONF_KEY,
                        TYPE_STRING,
                        NO_DEFAULT_VALUE,
                        CONN_URL_VALIDATOR,
                        IMPORTANCE_HIGH,
                        CONN_URL_CONF_DOC
                )
                .define(
                        CONTAINER_NAME_CONF_KEY,
                        TYPE_STRING,
                        CONTAINER_NAME_DEFAULT_VALUE,
                        CONTAINER_NAME_VALIDATOR,
                        IMPORTANCE_LOW,
                        CONTAINER_NAME_CONF_DOC
                )
                .define(
                        TOPIC_DIR,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        NON_EMPTY_STRING_VALIDATOR,
                        ConfigDef.Importance.HIGH,
                        TOPIC_DIR_DOC
                )
                .define( // Partition strategy configuration
                        PARTITION_STRATEGY_CONF,
                        ConfigDef.Type.STRING,
                        PARTITION_STRATEGY_DEFAULT,
                        NON_EMPTY_STRING_VALIDATOR,
                        ConfigDef.Importance.MEDIUM,
                        PARTITION_STRATEGY_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        PARTITION_STRATEGY_CONF,
                        PARTITION_STRATEGY_RECOMMENDER
                )
                .define( // Field-based partition strategy configuration
                        PARTITION_STRATEGY_FIELD_NAME_CONF,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        NON_EMPTY_STRING_VALIDATOR,
                        ConfigDef.Importance.MEDIUM,
                        PARTITION_STRATEGY_FIELD_NAME_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        PARTITION_STRATEGY_FIELD_NAME_CONF,
                        PARTITION_STRATEGY_FIELD_NAME_RECOMMENDER
                )
                .define(
                        PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        PARTITION_STRATEGY_TIME_PATH_FORMAT_DOC
                )
                .define(
                        PARTITION_STRATEGY_TIME_TIMEZONE_CONF,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        PARTITION_STRATEGY_TIME_TIMEZONE_DOC
                )
                .define(
                        PARTITION_STRATEGY_TIME_EXTRACTOR_CONF,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        PARTITION_STRATEGY_TIME_EXTRACTOR_DOC
                )
                .define(
                        PARTITION_STRATEGY_TIME_BUCKET_MS_CONF,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        PARTITION_STRATEGY_TIME_BUCKET_MS_DOC
                )
                .define(
                        ROLLOVER_POLICY_SIZE_CONF,
                        ConfigDef.Type.STRING,
                        "195000000000", // 195 GB, Max. supported by append blob
                        ConfigDef.Importance.LOW,
                        ROLLOVER_POLICY_SIZE_DOC
                );
    }

    public String getConnectionString() {
        return this.connectionString;
    }

    public String getContainerName() {
        return this.containerName;
    }

//    public String getBlobIdentifier() {
//        return this.blobIdentifier;
//    }

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
