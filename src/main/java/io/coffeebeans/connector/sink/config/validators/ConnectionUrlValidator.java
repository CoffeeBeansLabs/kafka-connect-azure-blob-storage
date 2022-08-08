package io.coffeebeans.connector.sink.config.validators;

import com.azure.storage.blob.BlobServiceClientBuilder;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

/**
 * This class will validate Connection URL String provided by the user as configuration parameter.
 */
public class ConnectionUrlValidator implements ConfigDef.Validator {

    /**
     * Ensure the provided connection string is not null, empty, blank and malformed.
     *
     * @param configurationKey Configuration key
     * @param configurationValue Connection String
     */
    @Override
    public void ensureValid(String configurationKey, Object configurationValue) {
        String connectionString = ((Password) configurationValue).value();

        try {

            // If the connection string is malformed or incorrect the azure sdk will throw an exception
            new BlobServiceClientBuilder()
                    .connectionString(connectionString)
                    .buildClient();

        } catch (IllegalArgumentException e) {
            throw new ConfigException("Invalid connection string: ", connectionString);
        }
    }
}
