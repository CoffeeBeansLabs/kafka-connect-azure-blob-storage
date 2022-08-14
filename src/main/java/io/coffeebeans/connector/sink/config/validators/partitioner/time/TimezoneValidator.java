package io.coffeebeans.connector.sink.config.validators.partitioner.time;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * {@link org.apache.kafka.common.config.ConfigDef.Validator} for
 * {@link io.coffeebeans.connector.sink.config.AzureBlobSinkConfig#TIMEZONE_CONF timezone} configuration.
 */
public class TimezoneValidator implements ConfigDef.Validator {

    /**
     * Validated timezone value.
     *
     * @param name name of configuration
     * @param value value
     */
    @Override
    public void ensureValid(String name, Object value) {
        String timezoneValue = (String) value;

        try {
            ZonedDateTime.now(ZoneId.of(timezoneValue));

        } catch (DateTimeException exception) {
            throw new ConfigException(name, value, "Invalid timezone");
        }
    }
}
