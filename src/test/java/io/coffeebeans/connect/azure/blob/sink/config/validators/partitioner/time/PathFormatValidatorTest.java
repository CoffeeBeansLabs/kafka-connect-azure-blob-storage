package io.coffeebeans.connect.azure.blob.sink.config.validators.partitioner.time;

import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PathFormatValidator}.
 */
public class PathFormatValidatorTest {
    private final String invalidPathFormat = "invalid";
    private final String validPathFormat = "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/'zone'=z";

    /**
     * <b>Method: {@link PathFormatValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumption: </b>
     * <ul>
     *     <li>Path format is invalid</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Should throw exception when the pathFormat value passed is invalid")
    public void ensureValid_givenInvalidPathFormat_shouldThrowException() {
        Assertions.assertThrows(ConfigException.class, () -> new PathFormatValidator().ensureValid(
                AzureBlobSinkConfig.PATH_FORMAT_CONF, invalidPathFormat)
        );
    }

    /**
     * <b>Method: {@link PathFormatValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumption: </b>
     * <ul>
     *     <li>Path format is valid</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should not throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Should not throw any exception when the pathFormat value passed is valid")
    public void ensureValid_givenValidPathFormat_shouldNotThrowException() {
        Assertions.assertDoesNotThrow(() -> new PathFormatValidator().ensureValid(
                AzureBlobSinkConfig.PATH_FORMAT_CONF, validPathFormat)
        );
    }
}
