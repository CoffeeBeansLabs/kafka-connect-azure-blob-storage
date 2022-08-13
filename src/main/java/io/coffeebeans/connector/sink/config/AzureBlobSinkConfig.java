package io.coffeebeans.connector.sink.config;

import static org.apache.kafka.common.config.ConfigDef.Importance;
import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;
import static org.apache.kafka.common.config.ConfigDef.Type;

import io.coffeebeans.connector.sink.config.recommenders.FileFormatRecommender;
import io.coffeebeans.connector.sink.config.recommenders.RetryTypeRecommender;
import io.coffeebeans.connector.sink.config.recommenders.RolloverFileSizeRecommender;
import io.coffeebeans.connector.sink.config.recommenders.format.AvroCodecRecommender;
import io.coffeebeans.connector.sink.config.recommenders.format.ParquetCodecRecommender;
import io.coffeebeans.connector.sink.config.recommenders.partitioner.StrategyRecommender;
import io.coffeebeans.connector.sink.config.recommenders.partitioner.field.FieldNameRecommender;
import io.coffeebeans.connector.sink.config.recommenders.partitioner.time.PathFormatRecommender;
import io.coffeebeans.connector.sink.config.recommenders.partitioner.time.TimestampExtractorRecommender;
import io.coffeebeans.connector.sink.config.recommenders.partitioner.time.TimezoneRecommender;
import io.coffeebeans.connector.sink.config.validators.ConnectionUrlValidator;
import io.coffeebeans.connector.sink.config.validators.ContainerNameValidator;
import io.coffeebeans.connector.sink.config.validators.GreaterThanZeroValidator;
import io.coffeebeans.connector.sink.config.validators.NonNegativeValidator;
import io.coffeebeans.connector.sink.config.validators.RetryTypeValidator;
import io.coffeebeans.connector.sink.config.validators.TopicsDirValueValidator;
import io.coffeebeans.connector.sink.config.validators.format.AvroCodecValidator;
import io.coffeebeans.connector.sink.config.validators.format.ParquetCodecValidator;
import io.coffeebeans.connector.sink.config.validators.partitioner.time.PathFormatValidator;
import io.coffeebeans.connector.sink.config.validators.partitioner.time.TimezoneValidator;
import io.coffeebeans.connector.sink.format.FileFormat;
import io.coffeebeans.connector.sink.partitioner.time.extractor.TimestampExtractorStrategy;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Validator;


/**
 * Class for defining connector configuration properties.
 */
public class AzureBlobSinkConfig extends AbstractConfig {

    // Configuration types
    private static final Type TYPE_STRING = Type.STRING;
    private static final Type TYPE_LONG = Type.LONG;
    private static final Type TYPE_INT = Type.INT;
    private static final Type TYPE_PASSWORD = Type.PASSWORD;

    // Configuration importance
    private static final Importance IMPORTANCE_LOW = Importance.LOW;
    private static final Importance IMPORTANCE_MEDIUM = Importance.MEDIUM;
    private static final Importance IMPORTANCE_HIGH = Importance.HIGH;


    // ###################################### Azure blob configurations ######################################
    /**
     * Azure Blob Connection URL related configurations.
     * No default value, order 1
     */
    public static final String CONN_URL_CONF_KEY = "connection.url";
    public static final Validator CONN_URL_VALIDATOR = new ConnectionUrlValidator();
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
    public static final String TOPICS_DIR_CONF_KEY = "topics.dir";
    public static final String TOPICS_DIR_DEFAULT_VALUE = "default";
    public static final Validator TOPICS_DIR_VALIDATOR = new TopicsDirValueValidator();
    public static final String TOPICS_DIR_CONF_DOC = "Parent directory where data ingested from kafka will be stored";


    // ###################################### Partition configurations ######################################
    /**
     * Partition strategy configuration.
     */
    public static final String PARTITION_STRATEGY_CONF_KEY = "partition.strategy";
    public static final String PARTITION_STRATEGY_DEFAULT_VALUE = "DEFAULT";
    public static final Recommender PARTITION_STRATEGY_RECOMMENDER = new StrategyRecommender();
    public static final String PARTITION_STRATEGY_CONF_DOC = "Partition strategy to be used";

    /**
     * Field name of the record from which the value will be extracted.
     * Visible only for field-based partitioning.
     * No default value, order 2
     */
    public static final String PARTITION_STRATEGY_FIELD_NAME_CONF_KEY = "partition.strategy.field.name";
    public static final String PARTITION_STRATEGY_FIELD_NAME_CONF_DOC = "Name of the field from which value should be"
            + " extracted";
    public static final Recommender PARTITION_STRATEGY_FIELD_NAME_RECOMMENDER = new FieldNameRecommender();

    /**
     * Path format for generating partition.
     * Visible only for time-based partitioning.
     */
    public static final String PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF_KEY = "path.format";
    public static final String PARTITION_STRATEGY_TIME_PATH_FORMAT_DEFAULT_VALUE = "'year'=YYYY/'month'=MM/'day'=dd/"
            + "'hour'=HH/'zone'=z";
    public static final Validator PARTITION_STRATEGY_TIME_PATH_FORMAT_VALIDATOR = new PathFormatValidator();
    public static final Recommender PARTITION_STRATEGY_TIME_PATH_FORMAT_RECOMMENDER = new PathFormatRecommender();
    public static final String PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF_DOC = "Output file path time partition format";


    /**
     * Timezone for time partitioner.
     * Visible only if partition strategy is time based.
     */
    public static final String PARTITION_STRATEGY_TIME_TIMEZONE_CONF_KEY = "timezone";
    public static final String PARTITION_STRATEGY_TIME_TIMEZONE_DEFAULT_VALUE = "UTC";
    public static final Validator PARTITION_STRATEGY_TIME_TIMEZONE_VALIDATOR = new TimezoneValidator();
    public static final Recommender PARTITION_STRATEGY_TIME_TIMEZONE_RECOMMENDER = new TimezoneRecommender();
    public static final String PARTITION_STRATEGY_TIME_TIMEZONE_CONF_DOC = "Timezone for the time partitioner";


    /**
     * Timestamp extractor configuration.
     * Visible only if partition strategy is time based.
     */
    public static final String PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_CONF_KEY = "timestamp.extractor";
    public static final String PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_DEFAULT_VALUE =
            TimestampExtractorStrategy.DEFAULT.toString();
    public static final Recommender PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_RECOMMENDER =
            new TimestampExtractorRecommender();
    public static final String PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_CONF_DOC = "Time extractor for time based "
            + "partitioner";


    // ###################################### File format configurations ######################################
    /**
     * File format related configuration.
     */
    public static final String FILE_FORMAT_CONF_KEY = "file.format";
    public static final String FILE_FORMAT_DEFAULT_VALUE = FileFormat.NONE.toString();
    public static final Recommender FILE_FORMAT_RECOMMENDER = new FileFormatRecommender();
    public static final String FILE_FORMAT_CONF_DOC = "Type of file format";


    // ###################################### Rolling file configurations ######################################
    /**
     * Rollover file policy related configurations.
     * Visible only if file format is NONE
     */
    public static final String ROLLOVER_POLICY_SIZE_CONF_KEY = "rollover.policy.size";
    public static final long ROLLOVER_POLICY_SIZE_DEFAULT_VALUE = 194000000000L;
    public static final Recommender ROLLOVER_POLICY_SIZE_RECOMMENDER = new RolloverFileSizeRecommender();
    public static final String ROLLOVER_POLICY_SIZE_CONF_DOC = "Maximum size of the blob for rollover to happen";

    /**
     * Number of records after which rotation should be done. -1 indicates
     * rotation should not happen based on number of records.
     */
    public static final String FLUSH_SIZE_CONF = "flush.size";
    public static final int FLUSH_SIZE_DEFAULT = -1;
    public static final String FLUSH_SIZE_DOC = "Number of records written to store before committing the file";

    /**
     * Time interval up to which a record writer is open, after which
     * rotation will be done.
     */
    public static final String ROTATION_INTERVAL_MS_CONF = "rotation.interval.ms";
    public static final long ROTATION_INTERVAL_MS_DEFAULT = -1L;
    public static final String ROTATION_INTERVAL_MS_DOC = "The time interval in ms after which file commit will be"
            + "invoked. The base time is set after first record is processed";

    /**
     * Part size will be used to create a buffer to store the
     * processed data.
     */
    public static final String PART_SIZE_CONF = "azblob.part.size";
    public static final int PART_SIZE_DEFAULT = 2000000; // 2MB
    public static final String PART_SIZE_DOC = "The size of the buffer to store the data of processed records by the"
            + "writer and this will also be the size of part upload to the blob storage";

    /**
     * Amount of data written after which rotation should happen.
     */
    public static final String FILE_SIZE_CONF = "azblob.file.size";
    public static final long FILE_SIZE_DEFAULT = 190000000000L; // 190 GB
    public static final String FILE_SIZE_DOC = "Maximum size of the blob after which tbe file will be committed";

    public static final String NULL_VALUE_BEHAVIOR_CONF = "behavior.on.null.values";
    public static final String NULL_VALUE_BEHAVIOR_DEFAULT = "IGNORE";
    public static final String NULL_VALUE_BEHAVIOR_DOC = "Action to perform when the connector receives null value";

    public static final String RETRY_TYPE_CONF = "azblob.retry.type";
    public static final String RETRY_TYPE_DEFAULT = "EXPONENTIAL";
    public static final String RETRY_TYPE_DOC = "Type of retry pattern to use";
    public static final Recommender RETRY_TYPE_RECOMMENDER = new RetryTypeRecommender();
    public static final Validator RETRY_TYPE_VALIDATOR = new RetryTypeValidator();

    public static final String RETRIES_CONF = "azblob.retry.retries";
    public static final int RETRIES_DEFAULT = 3;
    public static final String RETRIES_DOC = "Maximum number of retry attempts";
    public static final Validator RETRIES_VALIDATOR = new GreaterThanZeroValidator();

    public static final String CONNECTION_TIMEOUT_MS_CONF = "azblob.connection.timeout.ms";
    public static final long CONNECTION_TIMEOUT_MS_DEFAULT = 30_000L;
    public static final String CONNECTION_TIMEOUT_MS_DOC = "Maximum time the client will try each http call";
    public static final Validator CONNECTION_TIMEOUT_MS_VALIDATOR = new GreaterThanZeroValidator();

    public static final String RETRY_BACKOFF_MS_CONF = "azblob.retry.backoff.ms";
    public static final long RETRY_BACKOFF_MS_DEFAULT = 4_000L;
    public static final String RETRY_BACKOFF_MS_DOC = "Min delay before an operation";
    public static final Validator RETRY_BACKOFF_MS_VALIDATOR = new NonNegativeValidator();

    public static final String RETRY_MAX_BACKOFF_MS_CONF = "azblob.retry.max.backoff.ms";
    public static final long RETRY_MAX_BACKOFF_MS_DEFAULT = 120_000L;
    public static final String RETRY_MAX_BACKOFF_MS_DOC = "Max delay before an operation";
    public static final Validator RETRY_MAX_BACKOFF_MS_VALIDATOR = new GreaterThanZeroValidator();

    public static final String PARQUET_CODEC_CONF = "parquet.codec";
    public static final String PARQUET_CODEC_DEFAULT = "uncompressed";
    public static final String PARQUET_CODEC_DOC = "Compression codec for parquet files";
    public static final Recommender PARQUET_CODEC_RECOMMENDER = new ParquetCodecRecommender();
    public static final Validator PARQUET_CODEC_VALIDATOR = new ParquetCodecValidator();

    public static final String AVRO_CODEC_CONF = "avro.codec";
    public static final String AVRO_CODEC_DEFAULT = "null";
    public static final String AVRO_CODEC_DOC = "Compression codec for avro files";
    public static final Recommender AVRO_CODEC_RECOMMENDER = new AvroCodecRecommender();
    public static final Validator AVRO_CODEC_VALIDATOR = new AvroCodecValidator();

    public static final String SCHEMAS_CACHE_SIZE_CONF = "schemas.cache.config";
    public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
    public static final String SCHEMAS_CACHE_SIZE_DOC = "Size of schema cache for Avro Converter";
    public static final Validator SCHEMAS_CACHE_SIZE_VALIDATOR = new GreaterThanZeroValidator();

    /**
     * Not a configuration. It's a suffix which when concatenated with the topic name, will act
     * as a configuration (dynamic).
     *
     * <p>For example, if configured topics are: alpha, lambda
     * configuration for its schema can be done using:
     *      alpha.schema.url: url,
     *      lambda.schema.url: url
     *
     * <p>This configuration depends upon the file format.
     *
     * <p>Note:
     * This configuration is not recommended / validated by the connect-runtime.
     */
    public static final String TOPIC_SCHEMA_URL_SUFFIX = ".schema.url";


    // Common validators
    public static final Validator NON_EMPTY_STRING_VALIDATOR = new ConfigDef.NonEmptyString();


    private final String connectionString;
    private final String containerName;
    private final String topicsDir;
    private final String partitionStrategy;
    private final String fieldName;
    private final String pathFormat;
    private final String timezone;
    private final String timeExtractor;
    private final long rolloverFileSize;
    private final int flushSize;
    private final long rotationIntervalMs;
    private final int partSize;
    private final long fileSize;
    private final String fileFormat;
    private final String nullValueBehavior;
    private final String retryType;
    private final int maxRetries;
    private final long connectionTimeoutMs;
    private final long retryBackoffMs;
    private final long retryMaxBackoffMs;
    private final String parquetCompressionCodec;
    private final String avroCompressionCodec;
    private final int schemaCacheSize;

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
        this.connectionString = this.getPassword(CONN_URL_CONF_KEY).value();
        this.containerName = this.getString(CONTAINER_NAME_CONF_KEY);
        this.topicsDir = this.getString(TOPICS_DIR_CONF_KEY);
        this.partitionStrategy = this.getString(PARTITION_STRATEGY_CONF_KEY);
        this.fieldName = this.getString(PARTITION_STRATEGY_FIELD_NAME_CONF_KEY);
        this.pathFormat = this.getString(PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF_KEY);
        this.timezone = this.getString(PARTITION_STRATEGY_TIME_TIMEZONE_CONF_KEY);
        this.timeExtractor = this.getString(PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_CONF_KEY);
        this.rolloverFileSize = this.getLong(ROLLOVER_POLICY_SIZE_CONF_KEY);
        this.flushSize = this.getInt(FLUSH_SIZE_CONF);
        this.rotationIntervalMs = this.getLong(ROTATION_INTERVAL_MS_CONF);
        this.partSize = this.getInt(PART_SIZE_CONF);
        this.fileSize = this.getLong(FILE_SIZE_CONF);
        this.fileFormat = this.getString(FILE_FORMAT_CONF_KEY);
        this.nullValueBehavior = this.getString(NULL_VALUE_BEHAVIOR_CONF);
        this.retryType = this.getString(RETRY_TYPE_CONF);
        this.maxRetries = this.getInt(RETRIES_CONF);
        this.connectionTimeoutMs = this.getLong(CONNECTION_TIMEOUT_MS_CONF);
        this.retryBackoffMs = this.getLong(RETRY_BACKOFF_MS_CONF);
        this.retryMaxBackoffMs = this.getLong(RETRY_MAX_BACKOFF_MS_CONF);
        this.parquetCompressionCodec = this.getString(PARQUET_CODEC_CONF);
        this.avroCompressionCodec = this.getString(AVRO_CODEC_CONF);
        this.schemaCacheSize = this.getInt(SCHEMAS_CACHE_SIZE_CONF);
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
                        TYPE_PASSWORD,
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
                        TOPICS_DIR_CONF_KEY,
                        TYPE_STRING,
                        TOPICS_DIR_DEFAULT_VALUE,
                        TOPICS_DIR_VALIDATOR,
                        IMPORTANCE_LOW,
                        TOPICS_DIR_CONF_DOC
                )
                .define(
                        PARTITION_STRATEGY_CONF_KEY,
                        TYPE_STRING,
                        PARTITION_STRATEGY_DEFAULT_VALUE,
                        NON_EMPTY_STRING_VALIDATOR,
                        IMPORTANCE_MEDIUM,
                        PARTITION_STRATEGY_CONF_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        PARTITION_STRATEGY_CONF_KEY,
                        PARTITION_STRATEGY_RECOMMENDER
                )
                .define(
                        PARTITION_STRATEGY_FIELD_NAME_CONF_KEY,
                        TYPE_STRING,
                        NO_DEFAULT_VALUE,
                        NON_EMPTY_STRING_VALIDATOR,
                        IMPORTANCE_MEDIUM,
                        PARTITION_STRATEGY_FIELD_NAME_CONF_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        PARTITION_STRATEGY_FIELD_NAME_CONF_KEY,
                        PARTITION_STRATEGY_FIELD_NAME_RECOMMENDER
                )
                .define(
                        PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF_KEY,
                        TYPE_STRING,
                        PARTITION_STRATEGY_TIME_PATH_FORMAT_DEFAULT_VALUE,
                        PARTITION_STRATEGY_TIME_PATH_FORMAT_VALIDATOR,
                        IMPORTANCE_LOW,
                        PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF_KEY,
                        PARTITION_STRATEGY_TIME_PATH_FORMAT_RECOMMENDER
                )
                .define(
                        PARTITION_STRATEGY_TIME_TIMEZONE_CONF_KEY,
                        TYPE_STRING,
                        PARTITION_STRATEGY_TIME_TIMEZONE_DEFAULT_VALUE,
                        PARTITION_STRATEGY_TIME_TIMEZONE_VALIDATOR,
                        IMPORTANCE_LOW,
                        PARTITION_STRATEGY_TIME_TIMEZONE_CONF_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        PARTITION_STRATEGY_TIME_TIMEZONE_CONF_KEY,
                        PARTITION_STRATEGY_TIME_TIMEZONE_RECOMMENDER
                )
                .define(
                        PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_CONF_KEY,
                        TYPE_STRING,
                        PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_DEFAULT_VALUE,
                        IMPORTANCE_LOW,
                        PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_CONF_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_CONF_KEY,
                        PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_RECOMMENDER
                )
                .define(
                        FILE_FORMAT_CONF_KEY,
                        TYPE_STRING,
                        FILE_FORMAT_DEFAULT_VALUE,
                        IMPORTANCE_MEDIUM,
                        FILE_FORMAT_CONF_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        FILE_FORMAT_CONF_KEY,
                        FILE_FORMAT_RECOMMENDER
                )
                .define(
                        ROLLOVER_POLICY_SIZE_CONF_KEY,
                        TYPE_LONG,
                        ROLLOVER_POLICY_SIZE_DEFAULT_VALUE,
                        IMPORTANCE_LOW,
                        ROLLOVER_POLICY_SIZE_CONF_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        ROLLOVER_POLICY_SIZE_CONF_KEY,
                        ROLLOVER_POLICY_SIZE_RECOMMENDER
                ).define(
                        FLUSH_SIZE_CONF,
                        TYPE_INT,
                        FLUSH_SIZE_DEFAULT,
                        IMPORTANCE_LOW,
                        FLUSH_SIZE_DOC
                ).define(
                        ROTATION_INTERVAL_MS_CONF,
                        TYPE_LONG,
                        ROTATION_INTERVAL_MS_DEFAULT,
                        IMPORTANCE_LOW,
                        ROTATION_INTERVAL_MS_DOC
                ).define(
                        PART_SIZE_CONF,
                        TYPE_INT,
                        PART_SIZE_DEFAULT,
                        IMPORTANCE_LOW,
                        PART_SIZE_DOC
                ).define(
                        FILE_SIZE_CONF,
                        TYPE_LONG,
                        FILE_SIZE_DEFAULT,
                        IMPORTANCE_LOW,
                        FILE_SIZE_DOC
                ).define(
                        NULL_VALUE_BEHAVIOR_CONF,
                        TYPE_STRING,
                        NULL_VALUE_BEHAVIOR_DEFAULT,
                        IMPORTANCE_LOW,
                        NULL_VALUE_BEHAVIOR_DOC
                ).define(
                        RETRY_TYPE_CONF,
                        TYPE_STRING,
                        RETRY_TYPE_DEFAULT,
                        RETRY_TYPE_VALIDATOR,
                        IMPORTANCE_LOW,
                        RETRY_TYPE_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        RETRY_TYPE_CONF,
                        RETRY_TYPE_RECOMMENDER
                ).define(
                        RETRIES_CONF,
                        TYPE_INT,
                        RETRIES_DEFAULT,
                        RETRIES_VALIDATOR,
                        IMPORTANCE_LOW,
                        RETRIES_DOC
                ).define(
                        CONNECTION_TIMEOUT_MS_CONF,
                        TYPE_LONG,
                        CONNECTION_TIMEOUT_MS_DEFAULT,
                        CONNECTION_TIMEOUT_MS_VALIDATOR,
                        IMPORTANCE_MEDIUM,
                        CONNECTION_TIMEOUT_MS_DOC
                ).define(
                        RETRY_BACKOFF_MS_CONF,
                        TYPE_LONG,
                        RETRY_BACKOFF_MS_DEFAULT,
                        RETRY_BACKOFF_MS_VALIDATOR,
                        IMPORTANCE_LOW,
                        RETRY_BACKOFF_MS_DOC
                ).define(
                        RETRY_MAX_BACKOFF_MS_CONF,
                        TYPE_LONG,
                        RETRY_MAX_BACKOFF_MS_DEFAULT,
                        RETRY_MAX_BACKOFF_MS_VALIDATOR,
                        IMPORTANCE_LOW,
                        RETRY_MAX_BACKOFF_MS_DOC
                ).define(
                        PARQUET_CODEC_CONF,
                        TYPE_STRING,
                        PARQUET_CODEC_DEFAULT,
                        PARQUET_CODEC_VALIDATOR,
                        IMPORTANCE_LOW,
                        PARQUET_CODEC_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        PARQUET_CODEC_CONF,
                        PARQUET_CODEC_RECOMMENDER
                ).define(
                        AVRO_CODEC_CONF,
                        TYPE_STRING,
                        AVRO_CODEC_DEFAULT,
                        AVRO_CODEC_VALIDATOR,
                        IMPORTANCE_LOW,
                        AVRO_CODEC_DOC,
                        null,
                        -1,
                        ConfigDef.Width.NONE,
                        AVRO_CODEC_CONF,
                        AVRO_CODEC_RECOMMENDER
                ).define(
                        SCHEMAS_CACHE_SIZE_CONF,
                        TYPE_INT,
                        SCHEMAS_CACHE_SIZE_DEFAULT,
                        SCHEMAS_CACHE_SIZE_VALIDATOR,
                        IMPORTANCE_LOW,
                        SCHEMAS_CACHE_SIZE_DOC);
    }

    public String getConnectionString() {
        return this.connectionString;
    }

    public String getContainerName() {
        return this.containerName;
    }

    public String getTopicsDir() {
        return this.topicsDir;
    }

    public String getPartitionStrategy() {
        return this.partitionStrategy;
    }

    public String getFieldName() {
        return this.fieldName;
    }

    public String getPathFormat() {
        return pathFormat;
    }

    public String getTimezone() {
        return timezone;
    }

    public String getTimeExtractor() {
        return timeExtractor;
    }

    public long getRolloverFileSize() {
        return rolloverFileSize;
    }

    public int getFlushSize() {
        return this.flushSize;
    }

    public long getRotationIntervalMs() {
        return this.rotationIntervalMs;
    }

    public int getPartSize() {
        return this.partSize;
    }

    public long getFileSize() {
        return this.fileSize;
    }

    public String getFileFormat() {
        return this.fileFormat;
    }

    public String getNullValueBehavior() {
        return this.nullValueBehavior;
    }

    public String getRetryType() {
        return this.retryType;
    }

    public int getMaxRetries() {
        return this.maxRetries;
    }

    public long getConnectionTimeoutMs() {
        return this.connectionTimeoutMs;
    }

    public long getRetryBackoffMs() {
        return this.retryBackoffMs;
    }

    public long getRetryMaxBackoffMs() {
        return this.retryMaxBackoffMs;
    }

    public String getParquetCompressionCodec() {
        return this.parquetCompressionCodec;
    }

    public String getAvroCompressionCodec() {
        return this.avroCompressionCodec;
    }

    public int getSchemaCacheSize() {
        return this.schemaCacheSize;
    }
}
