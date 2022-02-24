package io.coffeebeans.connector.sink.config;

import io.coffeebeans.connector.sink.config.validators.ConnectionStringValidator;
import io.coffeebeans.connector.sink.config.validators.ContainerNameValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Validator;

import java.util.Map;

public class AzureBlobSinkConfig extends AbstractConfig {

    // Connection related configurations
    public static final String AZURE_BLOB_CONN_STRING_CONF = "connection.string";
    public static final Validator CONNECTION_STRING_VALIDATOR = new ConnectionStringValidator();

    // Container related configurations
    public static final String AZURE_BLOB_CONTAINER_NAME_CONF = "container.name";
    public static final String CONTAINER_NAME_DEFAULT = "default";
    public static final Validator CONTAINER_NAME_VALIDATOR = new ContainerNameValidator();

    // Blob related configurations
    public static final String AZURE_BLOB_IDENTIFIER_KEY = "blob.identifier.key";

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
        this.connectionString = this.getString(AZURE_BLOB_CONN_STRING_CONF);
        this.containerName = this.getString(AZURE_BLOB_CONTAINER_NAME_CONF);
        this.blobIdentifier = this.getString(AZURE_BLOB_IDENTIFIER_KEY);
    }


    public static ConfigDef getConfig() {
        ConfigDef configDef = new ConfigDef();
        defineConfig(configDef);

        return configDef;
    }

    public static void defineConfig(ConfigDef configDef) {
        configDef
                .define(
                        AZURE_BLOB_CONN_STRING_CONF,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        CONNECTION_STRING_VALIDATOR,
                        ConfigDef.Importance.HIGH,
                        "Connection string of the azure blob storage"
                )
                .define(
                        AZURE_BLOB_CONTAINER_NAME_CONF,
                        ConfigDef.Type.STRING,
                        CONTAINER_NAME_DEFAULT,
                        CONTAINER_NAME_VALIDATOR,
                        ConfigDef.Importance.HIGH,
                        "Container name to store the blobs"
                )
                .define(
                        AZURE_BLOB_IDENTIFIER_KEY,
                        ConfigDef.Type.STRING,
                        "blobName",
                        NON_EMPTY_STRING_VALIDATOR,
                        ConfigDef.Importance.MEDIUM,
                        "Key to identify blob"
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
