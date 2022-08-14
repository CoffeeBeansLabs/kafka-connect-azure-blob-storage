package io.coffeebeans.connector.sink.config.validators.format;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * {@link org.apache.kafka.common.config.ConfigDef.Validator} and
 * {@link org.apache.kafka.common.config.ConfigDef.Recommender} for
 * {@link io.coffeebeans.connector.sink.config.AzureBlobSinkConfig#COMPRESSION_LEVEL_CONF az.compression.level}.
 */
public class CompressionLevelValidator implements ConfigDef.Validator, ConfigDef.Recommender {
    private static final int MIN = -1;
    private static final int MAX = 9;
    private static final ConfigDef.Range validRange = ConfigDef.Range.between(MIN, MAX);

    /**
     * The valid values for the configuration given the current configuration values.
     *
     * @param name         The name of the configuration
     * @param parsedConfig The parsed configuration values
     * @return The list of valid values. To function properly, the returned objects should have the type
     * defined for the configuration using the recommender.
     */
    @Override
    public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
        return IntStream.range(MIN, MAX).boxed().collect(Collectors.toList());
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

    /**
     * Perform single configuration validation.
     *
     * @param name  The name of the configuration
     * @param value The value of the configuration
     * @throws ConfigException if the value is invalid.
     */
    @Override
    public void ensureValid(String name, Object value) throws ConfigException {
        validRange.ensureValid(name, value);
    }
}
