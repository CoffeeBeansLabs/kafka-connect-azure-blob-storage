package io.coffeebeans.connector.sink.config.validators.format;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.Format;
import io.coffeebeans.connector.sink.format.avro.AvroCodecs;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

/**
 * {@link ConfigDef.Validator} and {@link org.apache.kafka.common.config.ConfigDef.Recommender}
 * for `avro.codec` configuration.
 */
public class AvroCodecValidator implements ConfigDef.Validator, ConfigDef.Recommender {

    private static final List<String> AVRO_CODECS;
    private static final String ALLOWED_CODECS;

    static {
        /*
        Iterate through the Formats and add it to the list.
         */

        AvroCodecs[] codecs = AvroCodecs.values();

        AVRO_CODECS = Stream.of(codecs)
                .map(AvroCodecs::name)
                .collect(Collectors.toList());

        ALLOWED_CODECS = Utils.join(AVRO_CODECS, ", ");
    }

    /**
     * Perform single configuration validation.
     *
     * @param name  The name of the configuration
     * @param value The value of the configuration
     */
    @Override
    public void ensureValid(String name, Object value) {

        String configuredCodec = (String) value;

        for (String codec : AVRO_CODECS) {
            if (codec.equalsIgnoreCase(configuredCodec)) {
                return;
            }
        }
        throw new ConfigException(name, value, "String must be one of (case-insensitive): " + ALLOWED_CODECS);
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
        return Collections.singletonList(AVRO_CODECS);
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
        String format = (String) parsedConfig.get(
                AzureBlobSinkConfig.FORMAT_CONF
        );
        return Format.AVRO
                .toString()
                .equalsIgnoreCase(format);
    }
}
