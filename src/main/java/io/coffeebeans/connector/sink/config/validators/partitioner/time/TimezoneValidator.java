package io.coffeebeans.connector.sink.config.validators.partitioner.time;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Validator for Timezone config property.
 */
public class TimezoneValidator implements ConfigDef.Validator {

    /**
     * I will check if the timezone value passed is valid, if not I will throw an exception.
     *
     * @param timezoneConfKey Timezone configuration key
     * @param timezoneConfValue Timezone configuration value
     */
    @Override
    public void ensureValid(String timezoneConfKey, Object timezoneConfValue) {
        String timezoneValue = (String) timezoneConfValue;

        try {
            ZonedDateTime zonedDateTime = ZonedDateTime.now(ZoneId.of(timezoneValue));
        } catch (DateTimeException exception) {
            throw new ConfigException("Invalid timezone: ", timezoneValue);
        }
    }
}
