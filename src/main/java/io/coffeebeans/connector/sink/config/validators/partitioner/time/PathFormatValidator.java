package io.coffeebeans.connector.sink.config.validators.partitioner.time;

import java.time.format.DateTimeFormatter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * {@link org.apache.kafka.common.config.ConfigDef.Validator} for
 * {@link io.coffeebeans.connector.sink.config.AzureBlobSinkConfig#PATH_FORMAT_CONF path.format} configuration.
 */
public class PathFormatValidator implements ConfigDef.Validator {

    /**
     * Validates pattern.
     *
     * @param name name of the configuration
     * @param value value
     */
    @Override
    public void ensureValid(String name, Object value) {

        String pathFormatValue = (String) value;

        try {
            DateTimeFormatter.ofPattern(pathFormatValue);

        } catch (IllegalArgumentException exception) {
            throw new ConfigException(name, value, "Invalid pattern");
        }
    }
}
