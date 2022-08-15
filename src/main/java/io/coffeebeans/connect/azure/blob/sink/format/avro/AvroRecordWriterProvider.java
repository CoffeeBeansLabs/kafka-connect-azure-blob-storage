package io.coffeebeans.connect.azure.blob.sink.format.avro;

import static io.confluent.connect.avro.AvroDataConfig.CONNECT_META_DATA_CONFIG;
import static io.confluent.connect.avro.AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG;
import static io.confluent.connect.avro.AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG;

import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connect.azure.blob.sink.format.RecordWriter;
import io.coffeebeans.connect.azure.blob.sink.format.RecordWriterProvider;
import io.coffeebeans.connect.azure.blob.sink.format.SchemaStore;
import io.coffeebeans.connect.azure.blob.sink.storage.StorageManager;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.file.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link AvroRecordWriterProvider} is used to get instance of
 * {@link AvroRecordWriter}.
 */
public class AvroRecordWriterProvider implements RecordWriterProvider {
    private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
    private static final String EXTENSION = ".avro";

    private int blockSize;
    private AvroData avroData;
    private CodecFactory codecFactory;
    private final SchemaStore schemaStore;
    private final StorageManager storageManager;

    /**
     * Constructs {@link AvroRecordWriterProvider}.
     *
     * @param storageManager Storage manager to interact with Azure blob storage
     * @param schemaStore Schema store, only needed for JSON String or JSON without any schema
     */
    public AvroRecordWriterProvider(StorageManager storageManager, SchemaStore schemaStore) {

        this.schemaStore = schemaStore;
        this.storageManager = storageManager;
    }

    /**
     * Configures the {@link AvroRecordWriterProvider} based on the <br>
     * configurations passed by the user.
     *
     * @param config Connector configuration
     */
    @Override
    public void configure(AzureBlobSinkConfig config) {

        this.blockSize = config.getBlockSize();

        configureAvroData(config);
        configureCodecFactory(config);
    }

    /**
     * Instantiates and return instance of {@link AvroRecordWriter}.
     *
     * @param blobName Blob name
     * @param kafkaTopic Kafka topic
     * @return Instance of Avro Record writer
     */
    @Override
    public RecordWriter getRecordWriter(String blobName, String kafkaTopic) {

        String blobNameWithExtension = blobName + getExtension();

        return new AvroRecordWriter(
                storageManager,
                schemaStore,
                blockSize,
                blobNameWithExtension,
                kafkaTopic,
                codecFactory,
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
     * Configures the {@link CodecFactory}.<br>
     * It configures it based on the {@link AzureBlobSinkConfig#AVRO_CODEC_CONF avro.codec}<br>
     * property configured by the user.
     * <br>
     *
     * @param config Connector configuration
     */
    private void configureCodecFactory(AzureBlobSinkConfig config) {

        this.codecFactory = CodecFactory.fromString(
                config.getAvroCompressionCodec()
        );
        log.debug("Configured Avro compression codec: {}", config.getAvroCompressionCodec());
    }

    /**
     * Extension of the Avro files.
     *
     * @return Extension
     */
    public String getExtension() {
        return EXTENSION;
    }
}
