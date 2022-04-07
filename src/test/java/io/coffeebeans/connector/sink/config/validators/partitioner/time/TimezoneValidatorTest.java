package io.coffeebeans.connector.sink.config.validators.partitioner.time;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TimezoneValidatorTest {
    private final String validTimezone = "UTC";
    private final String invalidTimezone = "XYZ";

    @Test
    @DisplayName("Should throw exception when invalid timezone is passed")
    public void shouldThrowExceptionWhenInvalidTimezonePassed() {

        Assertions.assertThrows(ConfigException.class, () -> new TimezoneValidator().ensureValid(
                AzureBlobSinkConfig.PARTITION_STRATEGY_TIME_TIMEZONE_CONF_KEY, invalidTimezone)
        );
    }

    @Test
    @DisplayName("Should not throw exception when valid timezone is passed")
    public void shouldNotThrowExceptionWhenValidTimezonePassed() {

        Assertions.assertDoesNotThrow(() -> new TimezoneValidator().ensureValid(
                AzureBlobSinkConfig.PARTITION_STRATEGY_TIME_TIMEZONE_CONF_KEY, validTimezone)
        );
    }
}
