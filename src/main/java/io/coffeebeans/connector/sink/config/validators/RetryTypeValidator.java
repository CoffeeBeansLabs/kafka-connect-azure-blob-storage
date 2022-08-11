package io.coffeebeans.connector.sink.config.validators;

import com.azure.storage.common.policy.RetryPolicyType;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * {@link org.apache.kafka.common.config.ConfigDef.Validator} for `azblob.retry.type` configuration
 */
public class RetryTypeValidator implements ConfigDef.Validator {

    /**
     * Perform single configuration validation.
     *
     * @param name  The name of the configuration
     * @param value The value of the configuration
     */
    @Override
    public void ensureValid(String name, Object value) {

        String configuredPolicyType = (String) value;

        for (RetryPolicyType policyType : RetryPolicyType.values()) {
            if (policyType.toString().equalsIgnoreCase(configuredPolicyType)) {
                return;
            }
        }
        throw new ConfigException("Invalid retry policy type");
    }
}
