package io.coffeebeans.connector.sink.config.validators.partitioner.time;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class PathFormatValidatorTest {
    private final String invalidPathFormat = "invalid";
    private final String validPathFormat = "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/'zone'=z";

    @Test
    @DisplayName("Should throw exception when the pathFormat value passed is invalid")
    public void shouldThrowExceptionWhenInvalidPathFormatIsPassed() {
        Assertions.assertThrows(ConfigException.class, () -> new PathFormatValidator().ensureValid(
                AzureBlobSinkConfig.PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF_KEY, invalidPathFormat)
        );
    }

    @Test
    @DisplayName("Should not throw any exception when the pathFormat value passed is valid")
    public void shouldNotThrowExceptionWhenValidPathFormatIsPassed() {
        Assertions.assertDoesNotThrow(() -> new PathFormatValidator().ensureValid(
                AzureBlobSinkConfig.PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF_KEY, validPathFormat)
        );
    }
}
