package io.coffeebeans.connector.sink.config.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class ContainerNameValidator implements ConfigDef.Validator {

    /**
     * Ensure the provided container name is not null, empty and blank
     * @param containerNameConfig Container name config
     * @param containerNameValue Container name
     */
    @Override
    public void ensureValid(String containerNameConfig, Object containerNameValue) {
        String containerName = (String) containerNameValue;

        if (containerName == null || containerName.isEmpty() || containerName.isBlank()) {
            throw new ConfigException("Invalid container name: ", containerName);
        }
    }
}
