package io.coffeebeans.connector.sink.config.validators.partitioner.time;

import java.time.format.DateTimeFormatter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Validator for Path Format.
 */
public class PathFormatValidator implements ConfigDef.Validator {

    /**
     * Ensure the provided pathFormat value is valid.
     *
     * @param pathFormatKey pathFormat configuration key
     * @param pathFormatConfigValue pathFormat configuration value
     */
    @Override
    public void ensureValid(String pathFormatKey, Object pathFormatConfigValue) {
        String pathFormatValue = (String) pathFormatConfigValue;

        try {
            DateTimeFormatter.ofPattern(pathFormatValue);
        } catch (IllegalArgumentException exception) {
            throw new ConfigException("Invalid path.format : ", pathFormatValue);
        }
    }
}
