package io.coffeebeans.connector.sink.config.validators.format;

import io.coffeebeans.connector.sink.format.avro.SupportedAvroCodecs;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * {@link ConfigDef.Validator} for `avro.codec` configuration
 */
public class AvroCodecValidator implements ConfigDef.Validator {

    /**
     * Perform single configuration validation.
     *
     * @param name  The name of the configuration
     * @param value The value of the configuration
     */
    @Override
    public void ensureValid(String name, Object value) {

        String configuredAvroCodec = (String) value;

        for (SupportedAvroCodecs codec : SupportedAvroCodecs.values()) {
            if (codec.toString().equalsIgnoreCase(configuredAvroCodec)) {
                return;
            }
        }
        throw new ConfigException("Invalid compression codec");
    }
}
