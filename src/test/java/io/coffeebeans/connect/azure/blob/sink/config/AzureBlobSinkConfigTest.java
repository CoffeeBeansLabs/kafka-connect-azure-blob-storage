package io.coffeebeans.connect.azure.blob.sink.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Unit tests for {@link AzureBlobSinkConfig}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AzureBlobSinkConfigTest {
    private static final String CONN_STR_VALUE = "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuF"
            + "q2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProt"
            + "ocol=http;BlobEndpoint=http://host.docker.internal:10000/dev"
            + "storeaccount1;";

    private Map<String, String> parsedConfig;

    /**
     * Init.
     */
    @BeforeAll
    public void init() {
        parsedConfig = new HashMap<>();

        // Inserting 3 configs with no default values
        parsedConfig.put(AzureBlobSinkConfig.FORMAT_CONF, "Parquet");
        parsedConfig.put(AzureBlobSinkConfig.FLUSH_SIZE_CONF, "5000");
        parsedConfig.put(AzureBlobSinkConfig.CONNECTION_STRING_CONF, CONN_STR_VALUE);
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#ROTATE_INTERVAL_MS_CONF rotate.interval.ms}</b>
     */
    @Test
    @DisplayName("Configuration => rotate.interval.ms")
    void rotateIntervalMs_config() {
        long rotateIntervalMs = 9999L;
        Assertions.assertEquals(
                AzureBlobSinkConfig.ROTATE_INTERVAL_MS_DEFAULT, getConfig(parsedConfig).getRotateIntervalMs()
        );
        parsedConfig.put(AzureBlobSinkConfig.ROTATE_INTERVAL_MS_CONF, String.valueOf(rotateIntervalMs));
        assertEquals(
                rotateIntervalMs, getConfig(parsedConfig).getRotateIntervalMs()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#SCHEMA_CACHE_SIZE_CONF schema.cache.size}</b>
     */
    @Test
    @DisplayName("Configuration => schema.cache.size")
    void schemaCacheSize_config() {
        int schemaCacheSize = 500;
        Assertions.assertEquals(
                AzureBlobSinkConfig.SCHEMA_CACHE_SIZE_DEFAULT, getConfig(parsedConfig).getSchemaCacheSize()
        );
        parsedConfig.put(AzureBlobSinkConfig.SCHEMA_CACHE_SIZE_CONF, String.valueOf(schemaCacheSize));
        assertEquals(
                schemaCacheSize, getConfig(parsedConfig).getSchemaCacheSize()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#ENHANCED_AVRO_SCHEMA_SUPPORT_CONF enhanced.avro.schema.support}</b>
     */
    @Test
    @DisplayName("Configuration => enhanced.avro.schema.support")
    void enhancedAvroSchemaSupport_config() {
        boolean enhancedAvroSchemaSupport = true;
        Assertions.assertEquals(
                AzureBlobSinkConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT, getConfig(parsedConfig).isEnhancedSchemaSupportEnabled()
        );
        parsedConfig.put(AzureBlobSinkConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONF, String.valueOf(enhancedAvroSchemaSupport));
        assertEquals(
                enhancedAvroSchemaSupport, getConfig(parsedConfig).isEnhancedSchemaSupportEnabled()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#CONNECT_META_DATA_CONF connect.meta.data}</b>
     */
    @Test
    @DisplayName("Configuration => connect.meta.data")
    void connectMetaData_config() {
        boolean connectMetaData = false;
        Assertions.assertEquals(
                AzureBlobSinkConfig.CONNECT_META_DATA_DEFAULT, getConfig(parsedConfig).isConnectMetaDataEnabled()
        );
        parsedConfig.put(AzureBlobSinkConfig.CONNECT_META_DATA_CONF, String.valueOf(connectMetaData));
        assertEquals(
                connectMetaData, getConfig(parsedConfig).isConnectMetaDataEnabled()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#AVRO_CODEC_CONF avro.codec}</b>
     */
    @Test
    @DisplayName("Configuration => avro.codec")
    void avroCodec_config() {
        String avroCodec = "bzip2";
        Assertions.assertEquals(
                AzureBlobSinkConfig.AVRO_CODEC_DEFAULT, getConfig(parsedConfig).getAvroCompressionCodec()
        );
        parsedConfig.put(AzureBlobSinkConfig.AVRO_CODEC_CONF, avroCodec);
        assertEquals(
                avroCodec, getConfig(parsedConfig).getAvroCompressionCodec()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#PARQUET_CODEC_CONF parquet.codec}</b>
     */
    @Test
    @DisplayName("Configuration => parquet.codec")
    void parquetCodec_config() {
        String parquetCodec = "gzip";
        Assertions.assertEquals(
                AzureBlobSinkConfig.PARQUET_CODEC_DEFAULT, getConfig(parsedConfig).getParquetCompressionCodec()
        );
        parsedConfig.put(AzureBlobSinkConfig.PARQUET_CODEC_CONF, parquetCodec);
        assertEquals(
                parquetCodec, getConfig(parsedConfig).getParquetCompressionCodec()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#CONTAINER_NAME_CONF azblob.container.name}</b>
     */
    @Test
    @DisplayName("Configuration => azblob.container.name")
    void containerName_config() {
        String containerName = "test";
        Assertions.assertEquals(
                AzureBlobSinkConfig.CONTAINER_NAME_DEFAULT, getConfig(parsedConfig).getContainerName()
        );
        parsedConfig.put(AzureBlobSinkConfig.CONTAINER_NAME_CONF, containerName);
        assertEquals(
                containerName, getConfig(parsedConfig).getContainerName()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#FORMAT_BYTEARRAY_EXTENSION_CONF format.bytearray.extension}</b>
     */
    @Test
    @DisplayName("Configuration => format.bytearray.extension")
    void byteArrayExtension_config() {
        String extension = ".binary";
        Assertions.assertEquals(
                AzureBlobSinkConfig.FORMAT_BYTEARRAY_EXTENSION_DEFAULT, getConfig(parsedConfig).getBinaryFileExtension()
        );
        parsedConfig.put(AzureBlobSinkConfig.FORMAT_BYTEARRAY_EXTENSION_CONF, extension);
        assertEquals(
                extension, getConfig(parsedConfig).getBinaryFileExtension()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#BLOCK_SIZE_CONF azblob.block.size}</b>
     */
    @Test
    @DisplayName("Configuration => azblob.block.size")
    void blockSize_config() {
        int blockSize = 5242880;
        Assertions.assertEquals(
                AzureBlobSinkConfig.BLOCK_SIZE_DEFAULT, getConfig(parsedConfig).getBlockSize()
        );
        parsedConfig.put(AzureBlobSinkConfig.BLOCK_SIZE_CONF, String.valueOf(blockSize));
        assertEquals(
                blockSize, getConfig(parsedConfig).getBlockSize()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#RETRY_TYPE_CONF azblob.retry.type}</b>
     */
    @Test
    @DisplayName("Configuration => azblob.retry.type")
    void retryType_config() {
        String retryType = "fixed";
        Assertions.assertEquals(
                AzureBlobSinkConfig.RETRY_TYPE_DEFAULT, getConfig(parsedConfig).getRetryType()
        );
        parsedConfig.put(AzureBlobSinkConfig.RETRY_TYPE_CONF, retryType);
        assertEquals(
                retryType, getConfig(parsedConfig).getRetryType()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#RETRIES_CONF azblob.retry.retries}</b>
     */
    @Test
    @DisplayName("Configuration => azblob.retry.retries")
    void retries_config() {
        int retries = 10;
        Assertions.assertEquals(
                AzureBlobSinkConfig.RETRIES_DEFAULT, getConfig(parsedConfig).getMaxRetries()
        );
        parsedConfig.put(AzureBlobSinkConfig.RETRIES_CONF, String.valueOf(retries));
        assertEquals(
                retries, getConfig(parsedConfig).getMaxRetries()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#CONNECTION_TIMEOUT_MS_CONF azblob.connection.timeout.ms}</b>
     */
    @Test
    @DisplayName("Configuration => azblob.connection.timeout.ms")
    void connectionTimeoutMs_config() {
        long connectionTimoutMs = 10L;
        Assertions.assertEquals(
                AzureBlobSinkConfig.CONNECTION_TIMEOUT_MS_DEFAULT, getConfig(parsedConfig).getConnectionTimeoutMs()
        );
        parsedConfig.put(AzureBlobSinkConfig.CONNECTION_TIMEOUT_MS_CONF, String.valueOf(connectionTimoutMs));
        assertEquals(
                connectionTimoutMs, getConfig(parsedConfig).getConnectionTimeoutMs()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#COMPRESSION_TYPE_CONF az.compression.type}</b>
     */
    @Test
    @DisplayName("Configuration => az.compression.type")
    void compressionType_config() {
        String compressionType = "gzip";
        Assertions.assertEquals(
                AzureBlobSinkConfig.COMPRESSION_TYPE_DEFAULT, getConfig(parsedConfig).getCompressionType()
        );
        parsedConfig.put(AzureBlobSinkConfig.COMPRESSION_TYPE_CONF, compressionType);
        assertEquals(
                compressionType, getConfig(parsedConfig).getCompressionType()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#COMPRESSION_LEVEL_CONF az.compression.level}</b>
     */
    @Test
    @DisplayName("Configuration => az.compression.level")
    void compressionLevel_config() {
        int compressionLevel = 3;
        Assertions.assertEquals(
                AzureBlobSinkConfig.COMPRESSION_LEVEL_DEFAULT, getConfig(parsedConfig).getCompressionLevel()
        );
        parsedConfig.put(AzureBlobSinkConfig.COMPRESSION_LEVEL_CONF, String.valueOf(compressionLevel));
        assertEquals(
                compressionLevel, getConfig(parsedConfig).getCompressionLevel()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#RETRY_BACKOFF_MS_CONF azblob.retry.backoff.ms}</b>
     */
    @Test
    @DisplayName("Configuration => azblob.retry.backoff.ms")
    void retryBackoffMs_config() {
        long retryBackoffMs = 10_000L;
        Assertions.assertEquals(
                AzureBlobSinkConfig.RETRY_BACKOFF_MS_DEFAULT, getConfig(parsedConfig).getRetryBackoffMs()
        );
        parsedConfig.put(AzureBlobSinkConfig.RETRY_BACKOFF_MS_CONF, String.valueOf(retryBackoffMs));
        assertEquals(
                retryBackoffMs, getConfig(parsedConfig).getRetryBackoffMs()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#RETRY_MAX_BACKOFF_MS_CONF azblob.retry.max.backoff.ms}</b>
     */
    @Test
    @DisplayName("Configuration => azblob.retry.max.backoff.ms")
    void retryMaxBackoffMs_config() {
        long retryMaxBackoffMs = 5000L;
        Assertions.assertEquals(
                AzureBlobSinkConfig.RETRY_MAX_BACKOFF_MS_DEFAULT, getConfig(parsedConfig).getRetryMaxBackoffMs()
        );
        parsedConfig.put(AzureBlobSinkConfig.RETRY_MAX_BACKOFF_MS_CONF, String.valueOf(retryMaxBackoffMs));
        assertEquals(
                retryMaxBackoffMs, getConfig(parsedConfig).getRetryMaxBackoffMs()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#NULL_VALUE_BEHAVIOR_CONF null.value.behavior}</b>
     */
    @Test
    @DisplayName("Configuration => null.value.behavior")
    void nullValueBehavior_config() {
        String nullValueBehavior = "Fail";
        Assertions.assertEquals(
                AzureBlobSinkConfig.NULL_VALUE_BEHAVIOR_DEFAULT, getConfig(parsedConfig).getNullValueBehavior()
        );
        parsedConfig.put(AzureBlobSinkConfig.NULL_VALUE_BEHAVIOR_CONF, nullValueBehavior);
        assertEquals(
                nullValueBehavior, getConfig(parsedConfig).getNullValueBehavior()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#TOPICS_DIR_CONF topics.dir}</b>
     */
    @Test
    @DisplayName("Configuration => topics.dir")
    void topicsDir_config() {
        String topicsDir = "test";
        Assertions.assertEquals(
                AzureBlobSinkConfig.TOPICS_DIR_DEFAULT, getConfig(parsedConfig).getTopicsDir()
        );
        parsedConfig.put(AzureBlobSinkConfig.TOPICS_DIR_CONF, topicsDir);
        assertEquals(
                topicsDir, getConfig(parsedConfig).getTopicsDir()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#DIRECTORY_DELIM_CONF directory.delim}</b>
     */
    @Test
    @DisplayName("Configuration => directory.delim")
    void directoryDelim_config() {
        String directoryDelim = "-";
        Assertions.assertEquals(
                AzureBlobSinkConfig.DIRECTORY_DELIM_DEFAULT, getConfig(parsedConfig).getDirectoryDelim()
        );
        parsedConfig.put(AzureBlobSinkConfig.DIRECTORY_DELIM_CONF, directoryDelim);
        assertEquals(
                directoryDelim, getConfig(parsedConfig).getDirectoryDelim()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#PARTITION_STRATEGY_CONF partition.strategy}</b>
     */
    @Test
    @DisplayName("Configuration => partition.strategy")
    void partitionStrategy_config() {
        String partitionStrategy = "TIME";
        Assertions.assertEquals(
                AzureBlobSinkConfig.PARTITION_STRATEGY_DEFAULT, getConfig(parsedConfig).getPartitionStrategy()
        );
        parsedConfig.put(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF, partitionStrategy);
        assertEquals(
                partitionStrategy, getConfig(parsedConfig).getPartitionStrategy()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#PARTITION_FIELD_NAME_CONF partition.field.name}</b>
     */
    @Test
    @DisplayName("Configuration => partition.field.name")
    void partitionFieldName_config() {
        String partitionFieldName = "field-test";
        Assertions.assertEquals(
                AzureBlobSinkConfig.PARTITION_FIELD_NAME_DEFAULT, getConfig(parsedConfig).getFieldName()
        );
        parsedConfig.put(AzureBlobSinkConfig.PARTITION_FIELD_NAME_CONF, partitionFieldName);
        assertEquals(
                partitionFieldName, getConfig(parsedConfig).getFieldName()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#PATH_FORMAT_CONF path.format}</b>
     */
    @Test
    @DisplayName("Configuration => path.format")
    void pathFormat_config() {
        String pathFormat = "'year'=YYYY";
        Assertions.assertEquals(
                AzureBlobSinkConfig.PATH_FORMAT_DEFAULT, getConfig(parsedConfig).getPathFormat()
        );
        parsedConfig.put(AzureBlobSinkConfig.PATH_FORMAT_CONF, pathFormat);
        assertEquals(
                pathFormat, getConfig(parsedConfig).getPathFormat()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#TIMEZONE_CONF timezone}</b>
     */
    @Test
    @DisplayName("Configuration => timezone")
    void timezone_config() {
        String timezone = "Asia/Kolkata";
        Assertions.assertEquals(
                AzureBlobSinkConfig.TIMEZONE_DEFAULT, getConfig(parsedConfig).getTimezone()
        );
        parsedConfig.put(AzureBlobSinkConfig.TIMEZONE_CONF, timezone);
        assertEquals(
                timezone, getConfig(parsedConfig).getTimezone()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#TIMESTAMP_EXTRACTOR_CONF timestamp.extractor}</b>
     */
    @Test
    @DisplayName("Configuration => timestamp.extractor")
    void timestampExtractor_config() {
        String timestampExtractor = "Record_Field";
        Assertions.assertEquals(
                AzureBlobSinkConfig.TIMESTAMP_EXTRACTOR_DEFAULT, getConfig(parsedConfig).getTimestampExtractor()
        );
        parsedConfig.put(AzureBlobSinkConfig.TIMESTAMP_EXTRACTOR_CONF, timestampExtractor);
        assertEquals(
                timestampExtractor, getConfig(parsedConfig).getTimestampExtractor()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#TIMESTAMP_FIELD_CONF timestamp.field}</b>
     */
    @Test
    @DisplayName("Configuration => timestamp.field")
    void timestampField_config() {
        String timestampField = "test";
        Assertions.assertEquals(
                AzureBlobSinkConfig.TIMESTAMP_FIELD_DEFAULT, getConfig(parsedConfig).getTimestampField()
        );
        parsedConfig.put(AzureBlobSinkConfig.TIMESTAMP_FIELD_CONF, timestampField);
        assertEquals(
                timestampField, getConfig(parsedConfig).getTimestampField()
        );
    }

    /**
     * <b>Configuration: {@link AzureBlobSinkConfig#FILE_DELIM_CONF file.delim}</b>
     */
    @Test
    @DisplayName("Configuration => file.delim")
    void fileDelim_config() {
        String fileDelim = "*";
        Assertions.assertEquals(
                AzureBlobSinkConfig.FILE_DELIM_DEFAULT, getConfig(parsedConfig).getFileDelim()
        );
        parsedConfig.put(AzureBlobSinkConfig.FILE_DELIM_CONF, fileDelim);
        assertEquals(
                fileDelim, getConfig(parsedConfig).getFileDelim()
        );
    }

    private AzureBlobSinkConfig getConfig(Map<String, String> parsedConfig) {
        return new AzureBlobSinkConfig(parsedConfig);
    }
}
