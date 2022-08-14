package io.coffeebeans.connector.sink.config.validators;

import com.azure.storage.blob.BlobServiceClientBuilder;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

/**
 * {@link org.apache.kafka.common.config.ConfigDef.Validator} for
 * {@link io.coffeebeans.connector.sink.config.AzureBlobSinkConfig#CONNECTION_STRING_CONF azblob.connection.string}
 * configuration.
 */
public class ConnectionStringValidator implements ConfigDef.Validator {

    /**
     * Validates connection string.
     *
     * @param name name of the configuration
     * @param value value
     */
    @Override
    public void ensureValid(String name, Object value) {
        String connectionString = ((Password) value).value();

        try {

            // If the connection string is malformed or incorrect the azure sdk will throw an exception
            new BlobServiceClientBuilder()
                    .connectionString(connectionString)
                    .buildClient();

        } catch (IllegalArgumentException e) {
            throw new ConfigException(name, value, "Invalid connection string");
        }
    }
}
