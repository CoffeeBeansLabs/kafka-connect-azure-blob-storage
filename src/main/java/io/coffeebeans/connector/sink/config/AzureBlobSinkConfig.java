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

    // Common validators
    public static final Validator NON_EMPTY_STRING_VALIDATOR = new ConfigDef.NonEmptyString();


    // properties
    private final String connectionString;
    private final String containerName;
    private final String blobIdentifier;

    public AzureBlobSinkConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);
    }

    public AzureBlobSinkConfig(ConfigDef configDef, Map<String, String> parsedConfig) {
        super(configDef, parsedConfig);
        this.connectionString = this.getString(CONN_STRING_CONF);
        this.containerName = this.getString(CONTAINER_NAME_CONF);
        this.blobIdentifier = this.getString(BLOB_IDENTIFIER_KEY);
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
                        ConfigDef.Importance.HIGH,
                        CONTAINER_NAME_DOC
                )
                .define(
                        BLOB_IDENTIFIER_KEY,
                        ConfigDef.Type.STRING,
                        BLOB_IDENTIFIER_KEY_DEFAULT,
                        NON_EMPTY_STRING_VALIDATOR,
                        ConfigDef.Importance.MEDIUM,
                        BLOB_IDENTIFIER_KEY_DOC
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
}
