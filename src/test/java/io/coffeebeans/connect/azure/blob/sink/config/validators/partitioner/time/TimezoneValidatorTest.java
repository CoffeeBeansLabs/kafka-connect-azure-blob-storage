package io.coffeebeans.connect.azure.blob.sink.config.validators.partitioner.time;

import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TimezoneValidator}.
 */
public class TimezoneValidatorTest {
    private final String validTimezone = "UTC";
    private final String invalidTimezone = "XYZ";

    /**
     * <b>Method: {@link TimezoneValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumption: </b>
     * <ul>
     *     <li>Timzone is invalid</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Should throw exception when invalid timezone is passed")
    public void ensureValid_givenInvalidTimezone_shouldThrowException() {

        Assertions.assertThrows(ConfigException.class, () -> new TimezoneValidator().ensureValid(
                AzureBlobSinkConfig.TIMEZONE_CONF, invalidTimezone)
        );
    }

    /**
     * <b>Method: {@link TimezoneValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumption: </b>
     * <ul>
     *     <li>Timezone is valid</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should not throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Should not throw exception when valid timezone is passed")
    public void ensureValid_givenValidTimezone_shouldNotThrowException() {

        Assertions.assertDoesNotThrow(() -> new TimezoneValidator().ensureValid(
                AzureBlobSinkConfig.TIMEZONE_CONF, validTimezone)
        );
    }
}
