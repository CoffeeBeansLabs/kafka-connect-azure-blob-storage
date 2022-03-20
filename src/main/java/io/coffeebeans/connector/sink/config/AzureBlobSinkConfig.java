package io.coffeebeans.connector.sink.config;

import static org.apache.kafka.common.config.ConfigDef.Importance;
import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;
import static org.apache.kafka.common.config.ConfigDef.Type;

import io.coffeebeans.connector.sink.config.recommenders.PartitionStrategyFieldNameRecommender;
import io.coffeebeans.connector.sink.config.recommenders.PartitionStrategyRecommender;
import io.coffeebeans.connector.sink.config.validators.ConnectionStringValidator;
import io.coffeebeans.connector.sink.config.validators.ContainerNameValidator;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Validator;


/**
 * Class for defining connector configuration properties.
 */
public class AzureBlobSinkConfig extends AbstractConfig {

    private static final Type TYPE_STRING = Type.STRING;
    private static final Importance IMPORTANCE_LOW = Importance.LOW;
    private static final Importance IMPORTANCE_MEDIUM = Importance.MEDIUM;
    private static final Importance IMPORTANCE_HIGH = Importance.HIGH;


    /**
     * Azure Blob Connection related configurations.
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
     * Applicable only for field-based partitioning.
     */
    // TODO: Add support for List of fields
    public static final String PARTITION_STRATEGY_FIELD_NAME_CONF = "partition.strategy.field.name";
    public static final String PARTITION_STRATEGY_FIELD_NAME_DOC = "Name of the field from which values should be "
            + "extracted";
    public static final Recommender PARTITION_STRATEGY_FIELD_NAME_RECOMMENDER =
            new PartitionStrategyFieldNameRecommender();

    /**
     * Applicable only for time-based partitioning.
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
     * Rollover file policy related configurations.
     */
    public static final String ROLLOVER_POLICY_SIZE_CONF = "rollover.policy.size";
    public static final String ROLLOVER_POLICY_SIZE_DOC = "Maximum size of the blob for rollover to happen";

    public static final String MAXIMUM_BLOB_SIZE_SUPPORTED = "195000000000";

    // Common validators
    public static final Validator NON_EMPTY_STRING_VALIDATOR = new ConfigDef.NonEmptyString();


    private final String connectionString;
    private final String containerName;
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

    /**
     * I will take ConfigDef and Map of parsed configs. I will pass these parameters to super and will initialize the
     * fields.
     *
     * @param configDef ConfigDef
     * @param parsedConfig Map of parsed configs
     */
    public AzureBlobSinkConfig(ConfigDef configDef, Map<String, String> parsedConfig) {
        super(configDef, parsedConfig);
        this.connectionString = this.getString(CONN_URL_CONF_KEY);
        this.containerName = this.getString(CONTAINER_NAME_CONF_KEY);
        this.topicDir = this.getString(TOPIC_DIR);
        this.partitionStrategy = this.getString(PARTITION_STRATEGY_CONF);
        this.fieldName = this.getString(PARTITION_STRATEGY_FIELD_NAME_CONF);
        this.pathFormat = this.getString(PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF);
        this.timeBucket = this.getString(PARTITION_STRATEGY_TIME_BUCKET_MS_CONF);
        this.timezone = this.getString(PARTITION_STRATEGY_TIME_TIMEZONE_CONF);
        this.timeExtractor = this.getString(PARTITION_STRATEGY_TIME_EXTRACTOR_CONF);
        this.maxBlobSize = this.getString(ROLLOVER_POLICY_SIZE_CONF);
    }


    /**
     * I initialize a new ConfigDef and define it.
     *
     * @return ConfigDef
     */
    public static ConfigDef getConfig() {
        ConfigDef configDef = new ConfigDef();
        defineConfig(configDef);

        return configDef;
    }

    /**
     * I need a ConfigDef as parameter. I define the configuration properties in the ConfigDef like configuration key,
     * type of configuration value, default value, validator, importance, doc, recommender etc.
     *
     * @param configDef defined ConfigDef
     */
    public static void defineConfig(ConfigDef configDef) {
        configDef
                .define(
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
                        TYPE_STRING,
                        NO_DEFAULT_VALUE,
                        NON_EMPTY_STRING_VALIDATOR,
                        IMPORTANCE_HIGH,
                        TOPIC_DIR_DOC
                )
                .define(
                        PARTITION_STRATEGY_CONF,
                        TYPE_STRING,
                        PARTITION_STRATEGY_DEFAULT,
                        NON_EMPTY_STRING_VALIDATOR,
                        IMPORTANCE_MEDIUM,
                        PARTITION_STRATEGY_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        PARTITION_STRATEGY_CONF,
                        PARTITION_STRATEGY_RECOMMENDER
                )
                .define(
                        PARTITION_STRATEGY_FIELD_NAME_CONF,
                        TYPE_STRING,
                        NO_DEFAULT_VALUE,
                        NON_EMPTY_STRING_VALIDATOR,
                        IMPORTANCE_MEDIUM,
                        PARTITION_STRATEGY_FIELD_NAME_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        PARTITION_STRATEGY_FIELD_NAME_CONF,
                        PARTITION_STRATEGY_FIELD_NAME_RECOMMENDER
                )
                .define(
                        PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF,
                        TYPE_STRING,
                        null,
                        IMPORTANCE_LOW,
                        PARTITION_STRATEGY_TIME_PATH_FORMAT_DOC
                )
                .define(
                        PARTITION_STRATEGY_TIME_TIMEZONE_CONF,
                        TYPE_STRING,
                        null,
                        IMPORTANCE_LOW,
                        PARTITION_STRATEGY_TIME_TIMEZONE_DOC
                )
                .define(
                        PARTITION_STRATEGY_TIME_EXTRACTOR_CONF,
                        TYPE_STRING,
                        null,
                        IMPORTANCE_LOW,
                        PARTITION_STRATEGY_TIME_EXTRACTOR_DOC
                )
                .define(
                        PARTITION_STRATEGY_TIME_BUCKET_MS_CONF,
                        TYPE_STRING,
                        null,
                        IMPORTANCE_LOW,
                        PARTITION_STRATEGY_TIME_BUCKET_MS_DOC
                )
                .define(
                        ROLLOVER_POLICY_SIZE_CONF,
                        TYPE_STRING,
                        MAXIMUM_BLOB_SIZE_SUPPORTED, // 195 GB, Max. supported by append blob
                        IMPORTANCE_LOW,
                        ROLLOVER_POLICY_SIZE_DOC);
    }

    public String getConnectionString() {
        return this.connectionString;
    }

    public String getContainerName() {
        return this.containerName;
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
