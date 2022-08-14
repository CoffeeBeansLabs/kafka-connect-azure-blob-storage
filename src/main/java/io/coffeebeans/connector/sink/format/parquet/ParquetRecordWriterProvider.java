package io.coffeebeans.connector.sink.format.parquet;

import static io.confluent.connect.avro.AvroDataConfig.CONNECT_META_DATA_CONFIG;
import static io.confluent.connect.avro.AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG;
import static io.confluent.connect.avro.AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.RecordWriterProvider;
import io.coffeebeans.connector.sink.format.SchemaStore;
import io.coffeebeans.connector.sink.storage.StorageManager;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ParquetRecordWriterProvider} is used to get instance of
 * {@link ParquetRecordWriter}.
 */
public class ParquetRecordWriterProvider implements RecordWriterProvider {
    private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
    private static final String EXTENSION = ".parquet";

    private int blockSize;
    private AvroData avroData;
    private final SchemaStore schemaStore;
    private final StorageManager storageManager;
    private CompressionCodecName compressionCodec;

    /**
     * Constructs {@link ParquetRecordWriterProvider}.
     *
     * @param storageManager Storage manager to interact with Azure blob storage
     * @param schemaStore Schema store, needed only for JSON string or JSON without any schema
     */
    public ParquetRecordWriterProvider(StorageManager storageManager, SchemaStore schemaStore) {
        this.schemaStore = schemaStore;
        this.storageManager = storageManager;
    }

    /**
     * Configures the {@link ParquetRecordWriterProvider} based on the<br>
     * configurations passed by the user.
     *
     * @param config Connector configuration
     */
    @Override
    public void configure(AzureBlobSinkConfig config) {

        this.blockSize = config.getBlockSize();

        configureAvroData(config);
        configureCompressionCodec(config);
    }

    /**
     * Instantiates and return instance of {@link ParquetRecordWriter}.
     *
     * @param blobName Blob name (Prefixed with the directory info.)
     * @return Record Writer to write parquet files
     */
    @Override
    public RecordWriter getRecordWriter(String blobName, String kafkaTopic) {

        String blobNameWithExtension = blobName
                + this.compressionCodec.getExtension()
                + getExtension();

        return new ParquetRecordWriter(
                storageManager,
                schemaStore,
                blockSize,
                blobNameWithExtension,
                kafkaTopic,
                compressionCodec,
                avroData
        );
    }

    /**
     * Configures the {@link AvroData} with below properties.<br>
     * <br>
     * <table>
     *     <tr>
     *         <th style="padding: 0 15px">property</th>
     *         <th style="padding: 0 15px">configuration</th>
     *     </tr>
     *     <tr>
     *         <td style="padding: 0 15px">
     *             {@link AvroDataConfig#SCHEMAS_CACHE_SIZE_CONFIG SCHEMAS_CACHE_SIZE_CONFIG}
     *         </td>
     *         <td style="padding: 0 15px">
     *             {@link AzureBlobSinkConfig#SCHEMA_CACHE_SIZE_CONF schemas.cache.size}
     *         </td>
     *     </tr>
     *     <tr>
     *         <td style="padding: 0 15px">
     *             {@link AvroDataConfig#ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG}
     *         </td>
     *         <td style="padding: 0 15px">
     *             {@link AzureBlobSinkConfig#ENHANCED_AVRO_SCHEMA_SUPPORT_CONF enhanced.avro.schema.support}
     *         </td>
     *     </tr>
     *     <tr>
     *         <td style="padding: 0 15px">
     *             {@link AvroDataConfig#CONNECT_META_DATA_CONFIG CONNECT_META_DATA_CONFIG}
     *         </td>
     *         <td style="padding: 0 15px">
     *             {@link AzureBlobSinkConfig#CONNECT_META_DATA_CONF connect.meta.data}
     *         </td>
     *     </tr>
     * </table>
     * <br>
     *
     * @param config Connector configuration
     */
    private void configureAvroData(AzureBlobSinkConfig config) {

        Map<String, Object> props = new HashMap<>();
        props.put(SCHEMAS_CACHE_SIZE_CONFIG, config.getSchemaCacheSize());
        props.put(ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, config.isEnhancedSchemaSupportEnabled());
        props.put(CONNECT_META_DATA_CONFIG, config.isConnectMetaDataEnabled());

        this.avroData = new AvroData(
                new AvroDataConfig(props)
        );

        log.debug("Configured schema cache size: {}", config.getSchemaCacheSize());
    }

    /**
     * Configures the {@link CompressionCodecName}.<br>
     * It configures it based on the {@link AzureBlobSinkConfig#PARQUET_CODEC_CONF parquet.codec}<br>
     * property configured by the user.
     * <br>
     *
     * @param config Connector configuration
     */
    private void configureCompressionCodec(AzureBlobSinkConfig config) {

        this.compressionCodec = CompressionCodecName.fromConf(
                config.getParquetCompressionCodec()
        );

        log.debug("Configured compression codec: {}", config.getParquetCompressionCodec());
    }

    /**
     * Extension of Parquet file.<br>
     * It does not include extension of the compression codec.
     *
     * @return Parquet file extension
     */
    public String getExtension() {
        return EXTENSION;
    }
}