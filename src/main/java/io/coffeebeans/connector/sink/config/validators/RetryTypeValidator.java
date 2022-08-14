package io.coffeebeans.connector.sink.config.validators;

import com.azure.storage.common.policy.RetryPolicyType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

/**
 * {@link org.apache.kafka.common.config.ConfigDef.Validator} and
 * {@link org.apache.kafka.common.config.ConfigDef.Recommender} for
 * `azblob.retry.type` configuration.
 */
public class RetryTypeValidator implements ConfigDef.Validator, ConfigDef.Recommender {

    private static final List<String> RETRY_TYPES;
    private static final String ALLOWED_PATTERNS;

    static {
        /*
        Iterate through the RetryType and add it to the list.
         */

        RetryPolicyType[] types = RetryPolicyType.values();

        RETRY_TYPES = Stream.of(types)
                .map(RetryPolicyType::name)
                .collect(Collectors.toList());

        ALLOWED_PATTERNS = Utils.join(RETRY_TYPES, ", ");
    }

    /**
     * Perform single configuration validation.
     *
     * @param name  The name of the configuration
     * @param value The value of the configuration
     */
    @Override
    public void ensureValid(String name, Object value) {

        String configuredPolicyType = (String) value;

        for (String type : RETRY_TYPES) {
            if (type.equalsIgnoreCase(configuredPolicyType)) {
                return;
            }
        }
        throw new ConfigException(name, value, "String must be one of (case-insensitive): " + ALLOWED_PATTERNS);
    }

    /**
     * The valid values for the configuration given the current configuration values.
     *
     * @param name         The name of the configuration
     * @param parsedConfig The parsed configuration values
     * @return The list of valid values. To function properly, the returned objects should have the type
     *      defined for the configuration using the recommender.
     */
    @Override
    public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
        return Collections.singletonList(RETRY_TYPES);
    }

    /**
     * Set the visibility of the configuration given the current configuration values.
     *
     * @param name         The name of the configuration
     * @param parsedConfig The parsed configuration values
     * @return The visibility of the configuration
     */
    @Override
    public boolean visible(String name, Map<String, Object> parsedConfig) {
        return true;
    }
}
