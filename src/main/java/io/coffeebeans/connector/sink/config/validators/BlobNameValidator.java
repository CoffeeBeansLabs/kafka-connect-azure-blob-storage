package io.coffeebeans.connector.sink.config.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class BlobNameValidator implements ConfigDef.Validator {

    /**
     * Ensure the provided blob name is not null, empty and blank
     * @param blobNameConfig Blob name config
     * @param blobNameValue Blob name
     */
    @Override
    public void ensureValid(String blobNameConfig, Object blobNameValue) {
        String blobName = (String) blobNameValue;

        if (blobName == null || blobName.isEmpty() || blobName.isBlank()) {
            throw new ConfigException("Invalid blob name: ", blobName);
        }
    }
}
