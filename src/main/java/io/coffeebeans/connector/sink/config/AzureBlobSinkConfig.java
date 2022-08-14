package io.coffeebeans.connector.sink.config;

import static org.apache.kafka.common.config.ConfigDef.CaseInsensitiveValidString;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;
import static org.apache.kafka.common.config.ConfigDef.NonEmptyString;
import static org.apache.kafka.common.config.ConfigDef.Range;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.apache.kafka.common.config.ConfigDef.Width.NONE;

import io.coffeebeans.connector.sink.config.validators.ConnectionStringValidator;
import io.coffeebeans.connector.sink.config.validators.RetryTypeValidator;
import io.coffeebeans.connector.sink.config.validators.format.AvroCodecValidator;
import io.coffeebeans.connector.sink.config.validators.format.CompressionTypeValidator;
import io.coffeebeans.connector.sink.config.validators.format.FormatValidator;
import io.coffeebeans.connector.sink.config.validators.format.ParquetCodecValidator;
import io.coffeebeans.connector.sink.config.validators.partitioner.time.PathFormatValidator;
import io.coffeebeans.connector.sink.config.validators.partitioner.time.TimezoneValidator;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import io.coffeebeans.connector.sink.partitioner.time.extractor.TimestampExtractorStrategy;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Validator;


/**
 * Class for defining connector configuration properties.
 */
public class AzureBlobSinkConfig extends AbstractConfig {

    private static final String CONNECTOR_GROUP = "connector";
    private static final String AZURE_GROUP = "azure";
    private static final String STORAGE_GROUP = "storage";
    private static final String PARTITIONER_GROUP = "partitioner";

    // ###################################### Connector parameters ######################################

    public static final String FORMAT_CONF = "format";
    public static final String FORMAT_DOC = "Format of data which will be written to storage";
    public static final Object FORMAT_VALIDATOR = new FormatValidator();

    public static final String FLUSH_SIZE_CONF = "flush.size";
    public static final int FLUSH_SIZE_MIN_VALUE = 1;
    public static final String FLUSH_SIZE_DOC = "Number of records after which file commit will be invoked";
    public static final Validator FLUSH_SIZE_VALIDATOR = Range.atLeast(FLUSH_SIZE_MIN_VALUE);

    public static final String ROTATE_INTERVAL_MS_CONF = "rotate.interval.ms";
    public static final long ROTATE_INTERVAL_MS_DEFAULT = -1L;
    public static final String ROTATE_INTERVAL_MS_DOC = "The time interval in ms after which file commit will be"
            + "invoked. The base time is set after first record is processed";

    public static final String SCHEMA_CACHE_SIZE_CONF = "schema.cache.config";
    public static final int SCHEMA_CACHE_SIZE_DEFAULT = 1000;
    public static final String SCHEMA_CACHE_SIZE_DOC = "Size of schema cache for Avro Converter";
    public static final Validator SCHEMA_CACHE_SIZE_VALIDATOR = Range.atLeast(1);

    public static final String ENHANCED_AVRO_SCHEMA_SUPPORT_CONF = "enhanced.avro.schema.support";
    public static final boolean ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT = false;
    public static final String ENHANCED_AVRO_SCHEMA_SUPPORT_DOC = "Enhanced avro schema support. "
            + "Enum symbol preservation and Package Name awareness";

    public static final String CONNECT_META_DATA_CONF = "connect.meta.data";
    public static final boolean CONNECT_META_DATA_DEFAULT = true;
    public static final String CONNECT_META_DATA_DOC = "Allow the Connect converter to "
            + "add its metadata to the output schema";

    public static final String AVRO_CODEC_CONF = "avro.codec";
    public static final String AVRO_CODEC_DEFAULT = "null";
    public static final String AVRO_CODEC_DOC = "Compression codec for avro files";
    public static final Object AVRO_CODEC_VALIDATOR = new AvroCodecValidator();

    public static final String PARQUET_CODEC_CONF = "parquet.codec";
    public static final String PARQUET_CODEC_DEFAULT = "uncompressed";
    public static final String PARQUET_CODEC_DOC = "Compression codec for parquet files";
    public static final Object PARQUET_CODEC_VALIDATOR = new ParquetCodecValidator();


    // ###################################### Azure parameters ######################################

    public static final String CONNECTION_STRING_CONF = "azblob.connection.string";
    public static final String CONNECTION_STRING_DOC = "Connection string of the azure storage account";
    public static final Validator CONNECTION_STRING_VALIDATOR = new ConnectionStringValidator();

    public static final String CONTAINER_NAME_CONF = "azblob.container.name";
    public static final String CONTAINER_NAME_DEFAULT = "default";
    public static final String CONTAINER_NAME_DOC = "Name of the container where blobs will be stored";
    public static final Validator CONTAINER_NAME_VALIDATOR = new NonEmptyString();

    public static final String FORMAT_BYTEARRAY_EXTENSION_CONF = "format.bytearray.extension";
    public static final String FORMAT_BYTEARRAY_EXTENSION_DEFAULT = ".bin";
    public static final String FORMAT_BYTEARRAY_EXTENSION_DOC = "Extension for output binary files";

    public static final String BLOCK_SIZE_CONF = "azblob.block.size";
    public static final int BLOCK_SIZE_DEFAULT = 26214400; // 26 mb
    public static final String BLOCK_SIZE_DOC = "Block size of the Block-blob";
    public static final Validator BLOCK_SIZE_VALIDATOR = Range.between(5242880, 104857600);

    public static final String RETRY_TYPE_CONF = "azblob.retry.type";
    public static final String RETRY_TYPE_DEFAULT = "EXPONENTIAL";
    public static final String RETRY_TYPE_DOC = "Type of retry pattern to use";
    public static final Object RETRY_TYPE_VALIDATOR = new RetryTypeValidator();

    public static final String RETRIES_CONF = "azblob.retry.retries";
    public static final int RETRIES_DEFAULT = 3;
    public static final String RETRIES_DOC = "Maximum number of retry attempts";
    public static final Validator RETRIES_VALIDATOR = Range.atLeast(1);

    public static final String CONNECTION_TIMEOUT_MS_CONF = "azblob.connection.timeout.ms";
    public static final long CONNECTION_TIMEOUT_MS_DEFAULT = 30_000L;
    public static final String CONNECTION_TIMEOUT_MS_DOC = "Maximum time the client will try each http call";
    public static final Validator CONNECTION_TIMEOUT_MS_VALIDATOR = Range.between(1, 2147483647000L);

    public static final String COMPRESSION_TYPE_CONF = "az.compression.type";
    public static final String COMPRESSION_TYPE_DEFAULT = "none";
    public static final String COMPRESSION_TYPE_DOC = "Type of compression for formats that "
            + "do not support it out of the box";
    public static final Object COMPRESSION_TYPE_VALIDATOR = new CompressionTypeValidator();

    public static final String COMPRESSION_LEVEL_CONF = "az.compression.level";
    public static final int COMPRESSION_LEVEL_DEFAULT = -1;
    public static final String COMPRESSION_LEVEL_DOC = "Level of comrpession";
    public static final Validator COMPRESSION_LEVEL_VALIDATOR = Range.between(-1, 9);

    public static final String RETRY_BACKOFF_MS_CONF = "azblob.retry.backoff.ms";
    public static final long RETRY_BACKOFF_MS_DEFAULT = 4_000L;
    public static final String RETRY_BACKOFF_MS_DOC = "Min delay before an operation";
    public static final Validator RETRY_BACKOFF_MS_VALIDATOR = Range.atLeast(0L);

    public static final String RETRY_MAX_BACKOFF_MS_CONF = "azblob.retry.max.backoff.ms";
    public static final long RETRY_MAX_BACKOFF_MS_DEFAULT = 120_000L;
    public static final String RETRY_MAX_BACKOFF_MS_DOC = "Max delay before an operation";
    public static final Validator RETRY_MAX_BACKOFF_MS_VALIDATOR = Range.atLeast(1L);

    public static final String NULL_VALUE_BEHAVIOR_CONF = "behavior.on.null.values";
    public static final String NULL_VALUE_BEHAVIOR_DEFAULT = "IGNORE";
    public static final String NULL_VALUE_BEHAVIOR_DOC = "Action to perform when the connector receives null value";
    public static final Validator NULL_VALUE_BEHAVIOR_VALIDATOR = CaseInsensitiveValidString
            .in(
                    NullValueBehavior.IGNORE.toString(),
                    NullValueBehavior.FAIL.toString()
            );

    // ###################################### Storage parameters ######################################

    /**
     * Parent directory where blobs will be stored.
     */
    public static final String TOPICS_DIR_CONF = "topics.dir";
    public static final String TOPICS_DIR_DEFAULT = "topics";
    public static final Validator TOPICS_DIR_VALIDATOR = new NonEmptyString();
    public static final String TOPICS_DIR_DOC = "Parent directory where blobs will be stored";

    public static final String DIRECTORY_DELIM_CONF = "directory.delim";
    public static final String DIRECTORY_DELIM_DEFAULT = "/";
    public static final String DIRECTORY_DELIM_DOC = "Directory delimiter";

    public static final String FILE_DELIM_CONF = "file.delim";
    public static final String FILE_DELIM_DEFAULT = "+";
    public static final String FILE_DELIM_DOC = "File delimiter";


    // ###################################### Partition configurations ######################################

    /**
     * Partition strategy configuration.
     */
    public static final String PARTITION_STRATEGY_CONF = "partition.strategy";
    public static final String PARTITION_STRATEGY_DEFAULT = "DEFAULT";
    public static final String PARTITION_STRATEGY_DOC = "Partition strategy to be used";
    public static final Validator PARTITION_STRATEGY_VALIDATOR = CaseInsensitiveValidString
            .in(
                    PartitionStrategy.DEFAULT.toString(),
                    PartitionStrategy.TIME.toString(),
                    PartitionStrategy.FIELD.toString()
            );
    public static final List<String> PARTITION_STRATEGY_DEPENDANTS = List.of(
            "partition.field.name",
            "path.format",
            "timezone"
    );

    public static final String PARTITION_FIELD_NAME_CONF = "partition.field.name";
    public static final String PARTITION_FIELD_NAME_DEFAULT = "";
    public static final String PARTITION_FIELD_NAME_DOC = "Name of the field from which value should be extracted";

    public static final String PATH_FORMAT_CONF = "path.format";
    public static final String PATH_FORMAT_DEFAULT = "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/'zone'=z";
    public static final String PATH_FORMAT_DOC = "Converts timestamps into directory structure";
    public static final Validator PATH_FORMAT_VALIDATOR = new PathFormatValidator();

    public static final String TIMEZONE_CONF = "timezone";
    public static final String TIMEZONE_DEFAULT = "UTC";
    public static final String TIMEZONE_DOC = "Timezone for the time partitioner";
    public static final Validator TIMEZONE_VALIDATOR = new TimezoneValidator();

    public static final String TIMESTAMP_EXTRACTOR_CONF = "timestamp.extractor";
    public static final String TIMESTAMP_EXTRACTOR_DEFAULT = "DEFAULT";
    public static final String TIMESTAMP_EXTRACTOR_DOC = "Time extractor for time based partitioner";
    public static final Validator TIMESTAMP_EXTRACTOR_VALIDATOR = CaseInsensitiveValidString
            .in(
                    TimestampExtractorStrategy.DEFAULT.toString(),
                    TimestampExtractorStrategy.RECORD.toString(),
                    TimestampExtractorStrategy.RECORD_FIELD.toString()
            );

    public static final String TIMESTAMP_FIELD_CONF = "timestamp.field";
    public static final String TIMESTAMP_FIELD_DEFAULT = "";
    public static final String TIMESTAMP_FIELD_DOC = "Name of the field from which timestamp should be extracted";

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


    private final String format;
    private final int flushSize;
    private final long rotateIntervalMs;
    private final int schemaCacheSize;
    private final boolean enhancedAvroSchemaSupport;
    private final boolean connectMetaData;
    private final String avroCompressionCodec;
    private final String parquetCompressionCodec;

    private final String connectionString;
    private final String containerName;
    private final String binaryFileExtension;
    private final int blockSize;
    private final String retryType;
    private final int maxRetries;
    private final long connectionTimeoutMs;
    private final String compressionType;
    private final int compressionLevel;
    private final long retryBackoffMs;
    private final long retryMaxBackoffMs;
    private final String nullValueBehavior;

    private final String topicsDir;
    private final String directoryDelim;
    private final String fileDelim;

    private final String partitionStrategy;
    private final String fieldName;
    private final String pathFormat;
    private final String timezone;
    private final String timestampExtractor;
    private final String timestampField;

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

        this.format = this.getString(FORMAT_CONF);
        this.flushSize = this.getInt(FLUSH_SIZE_CONF);
        this.rotateIntervalMs = this.getLong(ROTATE_INTERVAL_MS_CONF);
        this.schemaCacheSize = this.getInt(SCHEMA_CACHE_SIZE_CONF);
        this.enhancedAvroSchemaSupport = this.getBoolean(ENHANCED_AVRO_SCHEMA_SUPPORT_CONF);
        this.connectMetaData = this.getBoolean(CONNECT_META_DATA_CONF);
        this.avroCompressionCodec = this.getString(AVRO_CODEC_CONF);
        this.parquetCompressionCodec = this.getString(PARQUET_CODEC_CONF);

        this.connectionString = this.getPassword(CONNECTION_STRING_CONF).value();
        this.containerName = this.getString(CONTAINER_NAME_CONF);
        this.binaryFileExtension = this.getString(FORMAT_BYTEARRAY_EXTENSION_CONF);
        this.blockSize = this.getInt(BLOCK_SIZE_CONF);
        this.retryType = this.getString(RETRY_TYPE_CONF);
        this.maxRetries = this.getInt(RETRIES_CONF);
        this.connectionTimeoutMs = this.getLong(CONNECTION_TIMEOUT_MS_CONF);
        this.compressionType = this.getString(COMPRESSION_TYPE_CONF);
        this.compressionLevel = this.getInt(COMPRESSION_LEVEL_CONF);
        this.retryBackoffMs = this.getLong(RETRY_BACKOFF_MS_CONF);
        this.retryMaxBackoffMs = this.getLong(RETRY_MAX_BACKOFF_MS_CONF);
        this.nullValueBehavior = this.getString(NULL_VALUE_BEHAVIOR_CONF);

        this.topicsDir = this.getString(TOPICS_DIR_CONF);
        this.directoryDelim = this.getString(DIRECTORY_DELIM_CONF);
        this.fileDelim = this.getString(FILE_DELIM_CONF);

        this.partitionStrategy = this.getString(PARTITION_STRATEGY_CONF);
        this.fieldName = this.getString(PARTITION_FIELD_NAME_CONF);
        this.pathFormat = this.getString(PATH_FORMAT_CONF);
        this.timezone = this.getString(TIMEZONE_CONF);
        this.timestampExtractor = this.getString(TIMESTAMP_EXTRACTOR_CONF);
        this.timestampField = this.getString(TIMESTAMP_FIELD_CONF);
    }


    /**
     * Initializes a new {@link ConfigDef} and define it.
     *
     * @return ConfigDef
     */
    public static ConfigDef getConfig() {
        ConfigDef configDef = new ConfigDef();
        defineConfig(configDef);

        return configDef;
    }

    /**
     * Define the configuration properties in the {@link ConfigDef} like.<br>
     * <ul>
     *     <li>Configuration key</li>
     *     <li>Type</li>
     *     <li>Default value</li>
     *     <li>Validator</li>
     *     <li>Importance</li>
     *     <li>Doc</li>
     *     <li>Configuration group</li>
     *     <li>Order in group</li>
     *     <li>Recommender</li>
     *     <li>Display name</li>
     *     <li>Dependants</li>
     * </ul>
     *
     * @param configDef defined ConfigDef
     */
    public static void defineConfig(ConfigDef configDef) {

        int connectorGroupOrder = 0;
        int azureGroupOrder = 0;
        int storageGroupOrder = 0;
        int partitionerGroupOrder = 0;

        configDef
                .define(
                        FORMAT_CONF,
                        STRING,
                        NO_DEFAULT_VALUE,
                        (Validator) FORMAT_VALIDATOR,
                        HIGH,
                        FORMAT_DOC,
                        CONNECTOR_GROUP,
                        ++connectorGroupOrder,
                        NONE,
                        FORMAT_CONF,
                        (Recommender) FORMAT_VALIDATOR
                )
                .define(
                        FLUSH_SIZE_CONF,
                        INT,
                        NO_DEFAULT_VALUE,
                        FLUSH_SIZE_VALIDATOR,
                        HIGH,
                        FLUSH_SIZE_DOC,
                        CONNECTOR_GROUP,
                        ++connectorGroupOrder,
                        NONE,
                        FLUSH_SIZE_CONF
                )
                .define(
                        ROTATE_INTERVAL_MS_CONF,
                        LONG,
                        ROTATE_INTERVAL_MS_DEFAULT,
                        HIGH,
                        ROTATE_INTERVAL_MS_DOC,
                        CONNECTOR_GROUP,
                        ++connectorGroupOrder,
                        NONE,
                        ROTATE_INTERVAL_MS_CONF
                )
                .define(
                        SCHEMA_CACHE_SIZE_CONF,
                        INT,
                        SCHEMA_CACHE_SIZE_DEFAULT,
                        SCHEMA_CACHE_SIZE_VALIDATOR,
                        LOW,
                        SCHEMA_CACHE_SIZE_DOC,
                        CONNECTOR_GROUP,
                        ++connectorGroupOrder,
                        NONE,
                        SCHEMA_CACHE_SIZE_CONF
                )
                .define(
                        ENHANCED_AVRO_SCHEMA_SUPPORT_CONF,
                        BOOLEAN,
                        ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT,
                        LOW,
                        ENHANCED_AVRO_SCHEMA_SUPPORT_DOC,
                        CONNECTOR_GROUP,
                        ++connectorGroupOrder,
                        NONE,
                        ENHANCED_AVRO_SCHEMA_SUPPORT_CONF
                )
                .define(
                        CONNECT_META_DATA_CONF,
                        BOOLEAN,
                        CONNECT_META_DATA_DEFAULT,
                        LOW,
                        CONNECT_META_DATA_DOC,
                        CONNECTOR_GROUP,
                        ++connectorGroupOrder,
                        NONE,
                        CONNECT_META_DATA_CONF
                )
                .define(
                        AVRO_CODEC_CONF,
                        STRING,
                        AVRO_CODEC_DEFAULT,
                        (Validator) AVRO_CODEC_VALIDATOR,
                        LOW,
                        AVRO_CODEC_DOC,
                        CONNECTOR_GROUP,
                        ++connectorGroupOrder,
                        NONE,
                        AVRO_CODEC_CONF,
                        (Recommender) AVRO_CODEC_VALIDATOR
                )
                .define(
                        PARQUET_CODEC_CONF,
                        STRING,
                        PARQUET_CODEC_DEFAULT,
                        (Validator) PARQUET_CODEC_VALIDATOR,
                        LOW,
                        PARQUET_CODEC_DOC,
                        CONNECTOR_GROUP,
                        ++connectorGroupOrder,
                        NONE,
                        PARQUET_CODEC_CONF,
                        (Recommender) PARQUET_CODEC_VALIDATOR
                )
                .define(
                        CONNECTION_STRING_CONF,
                        PASSWORD,
                        NO_DEFAULT_VALUE,
                        CONNECTION_STRING_VALIDATOR,
                        HIGH,
                        CONNECTION_STRING_DOC,
                        AZURE_GROUP,
                        ++azureGroupOrder,
                        ConfigDef.Width.LONG,
                        CONNECTION_STRING_CONF
                )
                .define(
                        CONTAINER_NAME_CONF,
                        STRING,
                        CONTAINER_NAME_DEFAULT,
                        CONTAINER_NAME_VALIDATOR,
                        MEDIUM,
                        CONTAINER_NAME_DOC,
                        AZURE_GROUP,
                        ++azureGroupOrder,
                        NONE,
                        CONTAINER_NAME_DOC
                )
                .define(
                        FORMAT_BYTEARRAY_EXTENSION_CONF,
                        STRING,
                        FORMAT_BYTEARRAY_EXTENSION_DEFAULT,
                        LOW,
                        FORMAT_BYTEARRAY_EXTENSION_DOC,
                        AZURE_GROUP,
                        ++azureGroupOrder,
                        NONE,
                        FORMAT_BYTEARRAY_EXTENSION_CONF
                )
                .define(
                        BLOCK_SIZE_CONF,
                        INT,
                        BLOCK_SIZE_DEFAULT,
                        BLOCK_SIZE_VALIDATOR,
                        HIGH,
                        BLOCK_SIZE_DOC,
                        AZURE_GROUP,
                        ++azureGroupOrder,
                        NONE,
                        BLOCK_SIZE_CONF
                )
                .define(
                        RETRY_TYPE_CONF,
                        STRING,
                        RETRY_TYPE_DEFAULT,
                        (Validator) RETRY_TYPE_VALIDATOR,
                        LOW,
                        RETRY_TYPE_DOC,
                        AZURE_GROUP,
                        ++azureGroupOrder,
                        NONE,
                        RETRY_TYPE_CONF,
                        (Recommender) RETRY_TYPE_VALIDATOR
                )
                .define(
                        RETRIES_CONF,
                        INT,
                        RETRIES_DEFAULT,
                        RETRIES_VALIDATOR,
                        MEDIUM,
                        RETRIES_DOC,
                        AZURE_GROUP,
                        ++azureGroupOrder,
                        NONE,
                        RETRIES_CONF
                )
                .define(
                        CONNECTION_TIMEOUT_MS_CONF,
                        LONG,
                        CONNECTION_TIMEOUT_MS_DEFAULT,
                        CONNECTION_TIMEOUT_MS_VALIDATOR,
                        MEDIUM,
                        CONNECTION_TIMEOUT_MS_DOC,
                        AZURE_GROUP,
                        ++azureGroupOrder,
                        NONE,
                        CONNECTION_TIMEOUT_MS_CONF
                )
                .define(
                        COMPRESSION_TYPE_CONF,
                        STRING,
                        COMPRESSION_TYPE_DEFAULT,
                        (Validator) COMPRESSION_TYPE_VALIDATOR,
                        LOW,
                        COMPRESSION_TYPE_DOC,
                        AZURE_GROUP,
                        ++azureGroupOrder,
                        NONE,
                        COMPRESSION_TYPE_CONF,
                        (Recommender) COMPRESSION_TYPE_VALIDATOR
                )
                .define(
                        COMPRESSION_LEVEL_CONF,
                        INT,
                        COMPRESSION_LEVEL_DEFAULT,
                        COMPRESSION_LEVEL_VALIDATOR,
                        LOW,
                        COMPRESSION_LEVEL_DOC,
                        AZURE_GROUP,
                        ++azureGroupOrder,
                        NONE,
                        COMPRESSION_TYPE_CONF
                )
                .define(
                        RETRY_BACKOFF_MS_CONF,
                        LONG,
                        RETRY_BACKOFF_MS_DEFAULT,
                        RETRY_BACKOFF_MS_VALIDATOR,
                        LOW,
                        RETRY_BACKOFF_MS_DOC,
                        AZURE_GROUP,
                        ++azureGroupOrder,
                        NONE,
                        RETRY_BACKOFF_MS_CONF
                )
                .define(
                        RETRY_MAX_BACKOFF_MS_CONF,
                        LONG,
                        RETRY_MAX_BACKOFF_MS_DEFAULT,
                        RETRY_MAX_BACKOFF_MS_VALIDATOR,
                        LOW,
                        RETRY_MAX_BACKOFF_MS_DOC,
                        AZURE_GROUP,
                        ++azureGroupOrder,
                        NONE,
                        RETRY_MAX_BACKOFF_MS_CONF
                )
                .define(
                        NULL_VALUE_BEHAVIOR_CONF,
                        STRING,
                        NULL_VALUE_BEHAVIOR_DEFAULT,
                        NULL_VALUE_BEHAVIOR_VALIDATOR,
                        LOW,
                        NULL_VALUE_BEHAVIOR_DOC,
                        AZURE_GROUP,
                        ++azureGroupOrder,
                        NONE,
                        NULL_VALUE_BEHAVIOR_CONF
                )
                .define(
                        TOPICS_DIR_CONF,
                        STRING,
                        TOPICS_DIR_DEFAULT,
                        TOPICS_DIR_VALIDATOR,
                        HIGH,
                        TOPICS_DIR_DOC,
                        STORAGE_GROUP,
                        ++storageGroupOrder,
                        NONE,
                        TOPICS_DIR_CONF
                )
                .define(
                        DIRECTORY_DELIM_CONF,
                        STRING,
                        DIRECTORY_DELIM_DEFAULT,
                        MEDIUM,
                        DIRECTORY_DELIM_DOC,
                        STORAGE_GROUP,
                        ++storageGroupOrder,
                        NONE,
                        DIRECTORY_DELIM_CONF
                )
                .define(
                        FILE_DELIM_CONF,
                        STRING,
                        FILE_DELIM_DEFAULT,
                        MEDIUM,
                        FILE_DELIM_DOC,
                        STORAGE_GROUP,
                        ++storageGroupOrder,
                        NONE,
                        DIRECTORY_DELIM_CONF
                )
                .define(
                        PARTITION_STRATEGY_CONF,
                        STRING,
                        PARTITION_STRATEGY_DEFAULT,
                        PARTITION_STRATEGY_VALIDATOR,
                        HIGH,
                        PARTITION_STRATEGY_DOC,
                        PARTITIONER_GROUP,
                        ++partitionerGroupOrder,
                        NONE,
                        PARTITION_STRATEGY_CONF,
                        PARTITION_STRATEGY_DEPENDANTS
                )
                .define(
                        PARTITION_FIELD_NAME_CONF,
                        STRING,
                        PARTITION_FIELD_NAME_DEFAULT,
                        MEDIUM,
                        PARTITION_FIELD_NAME_DOC,
                        PARTITIONER_GROUP,
                        ++partitionerGroupOrder,
                        NONE,
                        PARTITION_FIELD_NAME_CONF
                )
                .define(
                        PATH_FORMAT_CONF,
                        STRING,
                        PATH_FORMAT_DEFAULT,
                        PATH_FORMAT_VALIDATOR,
                        MEDIUM,
                        PATH_FORMAT_DOC,
                        PARTITIONER_GROUP,
                        ++partitionerGroupOrder,
                        NONE,
                        PATH_FORMAT_CONF
                )
                .define(
                        TIMEZONE_CONF,
                        STRING,
                        TIMEZONE_DEFAULT,
                        TIMEZONE_VALIDATOR,
                        MEDIUM,
                        TIMEZONE_DOC,
                        PARTITIONER_GROUP,
                        ++partitionerGroupOrder,
                        NONE,
                        TIMEZONE_CONF
                )
                .define(
                        TIMESTAMP_EXTRACTOR_CONF,
                        STRING,
                        TIMESTAMP_EXTRACTOR_DEFAULT,
                        TIMESTAMP_EXTRACTOR_VALIDATOR,
                        MEDIUM,
                        TIMESTAMP_EXTRACTOR_DOC,
                        PARTITIONER_GROUP,
                        ++partitionerGroupOrder,
                        NONE,
                        TIMESTAMP_EXTRACTOR_CONF
                )
                .define(
                        TIMESTAMP_FIELD_CONF,
                        STRING,
                        TIMESTAMP_FIELD_DEFAULT,
                        MEDIUM,
                        TIMESTAMP_FIELD_DOC,
                        PARTITIONER_GROUP,
                        ++partitionerGroupOrder,
                        NONE,
                        TIMESTAMP_FIELD_CONF);
    }

    public String getFormat() {
        return this.format;
    }

    public int getFlushSize() {
        return this.flushSize;
    }

    public long getRotateIntervalMs() {
        return this.rotateIntervalMs;
    }

    public int getSchemaCacheSize() {
        return this.schemaCacheSize;
    }

    public boolean isEnhancedSchemaSupportEnabled() {
        return this.enhancedAvroSchemaSupport;
    }

    public boolean isConnectMetaDataEnabled() {
        return this.connectMetaData;
    }

    public String getAvroCompressionCodec() {
        return this.avroCompressionCodec;
    }

    public String getParquetCompressionCodec() {
        return this.parquetCompressionCodec;
    }

    public String getConnectionString() {
        return this.connectionString;
    }

    public String getContainerName() {
        return this.containerName;
    }

    public String getBinaryFileExtension() {
        return this.binaryFileExtension;
    }

    public int getBlockSize() {
        return this.blockSize;
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

    public String getCompressionType() {
        return this.compressionType;
    }

    public int getCompressionLevel() {
        return this.compressionLevel;
    }

    public String getNullValueBehavior() {
        return this.nullValueBehavior;
    }

    public long getRetryBackoffMs() {
        return this.retryBackoffMs;
    }

    public long getRetryMaxBackoffMs() {
        return this.retryMaxBackoffMs;
    }

    public String getTopicsDir() {
        return this.topicsDir;
    }

    public String getDirectoryDelim() {
        return this.directoryDelim;
    }

    public String getFileDelim() {
        return this.fileDelim;
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
        return this.timestampExtractor;
    }

    public String getTimestampField() {
        return this.timestampField;
    }
}
