package io.coffeebeans.connector.sink.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.NonEmptyString;
import org.apache.kafka.common.config.ConfigDef.Validator;

import java.util.Map;

public class AzureBlobSinkConfig extends AbstractConfig {
    public static final String AZURE_BLOB_CONN_STRING_CONF = "azure.blob.connection";
    public static final String AZURE_BLOB_CONTAINER_NAME_CONF = "azure.blob.container.name";
    public static final Validator NON_EMPTY_STRING_VALIDATOR = new NonEmptyString();

    private final String connectionString;
    private final String containerName;

    public AzureBlobSinkConfig(ConfigDef configDef, Map<String, String> parsedConfig) {
        super(configDef, parsedConfig);
        this.connectionString = this.getString(AZURE_BLOB_CONN_STRING_CONF);
        this.containerName = this.getString(AZURE_BLOB_CONTAINER_NAME_CONF);
    }

    public AzureBlobSinkConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);
    }

    public static ConfigDef getConfig() {
        ConfigDef configDef = new ConfigDef();
        defineConnectionConfig(configDef);

        return configDef;
    }

    public static void defineConnectionConfig(ConfigDef configDef) {
        configDef
                .define(
                        AZURE_BLOB_CONN_STRING_CONF,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        NON_EMPTY_STRING_VALIDATOR,
                        ConfigDef.Importance.HIGH,
                        "Connection string of the azure blob storage"
                )
                .define(
                        AZURE_BLOB_CONTAINER_NAME_CONF,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        NON_EMPTY_STRING_VALIDATOR,
                        ConfigDef.Importance.HIGH,
                        "Container name to store the blobs"
                );
    }

    public String getConnectionString() {
        return this.connectionString;
    }

    public String getContainerName() {
        return this.containerName;
    }
}
