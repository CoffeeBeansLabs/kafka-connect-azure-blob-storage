package io.coffeebeans.connect.azure.blob.sink.config.validators.format;

import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connect.azure.blob.sink.format.Format;
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
 * {@link AzureBlobSinkConfig#FORMAT_CONF format} configuration.
 */
public class FormatValidator implements ConfigDef.Validator, ConfigDef.Recommender {

    private static final List<Object> FORMATS;
    private static final String ALLOWED_FORMATS;

    static {
        /*
        Iterate through the Formats and add it to the list.
         */

        Format[] formatValues = Format.values();

        FORMATS = Stream.of(formatValues)
                    .map(Format::name)
                    .collect(Collectors.toList());

        ALLOWED_FORMATS = Utils.join(FORMATS, ", ");
    }

    /**
     * Returns a list of supported formats by the connector.
     *
     * @param name format conf
     * @param parsedConfig Parsed config
     * @return List of allowed formats
     */
    @Override
    public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
        return FORMATS;
    }

    /**
     * Is this configuration recommended.
     *
     * @param s Conf key
     * @param map Map of user passed configurations
     * @return True if recommended else false
     */
    @Override
    public boolean visible(String s, Map<String, Object> map) {
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
        String configuredFormat = ((String) value).trim();
        for (Object format : FORMATS) {
            String castedFormat = (String) format;
            if (castedFormat.equalsIgnoreCase(configuredFormat)) {
                return;
            }
        }
        throw new ConfigException(name, value, "String must be one of (case-insensitive): " + ALLOWED_FORMATS);
    }

    @Override
    public String toString() {
        return "[" + ALLOWED_FORMATS + "]";
    }
}
