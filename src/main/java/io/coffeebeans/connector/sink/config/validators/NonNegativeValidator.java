package io.coffeebeans.connector.sink.config.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * {@link ConfigDef.Validator} for configurations which require value to be <br>
 * positive.
 */
public class NonNegativeValidator implements ConfigDef.Validator {

    /**
     * Perform single configuration validation.
     *
     * @param name  The name of the configuration
     * @param value The value of the configuration
     */
    @Override
    public void ensureValid(String name, Object value) {

        if (value instanceof Integer && (int) value >= 0) {
            return;

        } else if (value instanceof Long && (long) value >= 0) {
            return;

        } else if (value instanceof Double && (double) value >= 0) {
            return;

        } else if (value instanceof Float && (float) value >= 0) {
            return;
        }
        throw new ConfigException("Cannot be negative");
    }
}
