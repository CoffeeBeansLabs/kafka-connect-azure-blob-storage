package io.coffeebeans.connector.sink.config.validators;

import com.azure.storage.blob.BlobServiceClientBuilder;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * This class will validate Connection URL String provided by the user as configuration parameter.
 */
public class ConnectionStringValidator implements ConfigDef.Validator {

    /**
     * Ensure the provided connection string is not null, empty, blank and malformed.
     *
     * @param configurationKey Configuration key
     * @param configurationValue Connection String
     */
    @Override
    public void ensureValid(String configurationKey, Object configurationValue) {
        String connectionString = (String) configurationValue;

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
