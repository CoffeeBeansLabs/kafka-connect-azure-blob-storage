package io.coffeebeans.connect.azure.blob.sink.config.validators.format;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.coffeebeans.connect.azure.blob.sink.format.Format;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;


/**
 * Unit tests for {@link FormatValidator}.
 */
public class FormatValidatorTest {

    private final FormatValidator validator = new FormatValidator();

    /**
     * <b>Method: {@link FormatValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Valid format</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should not throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given valid format, should not throw exception")
    void ensureValid_givenValidFormat_shouldNotThrowException() {

        String format = "Parquet";
        assertDoesNotThrow(() -> validator.ensureValid("", format));
    }

    /**
     * <b>Method: {@link FormatValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Invalid format</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given Invalid format, should throw exception")
    void ensureValid_givenInvalidFormat_shouldThrowException() {

        String format = "Invalid";
        assertThrows(ConfigException.class, () -> validator.ensureValid("", format));
    }

    /**
     * <b>Method: {@link FormatValidator#validValues(String, Map)}</b>.<br>
     * <b>Expectations: </b>
     * <ul>
     *     <li>Should return valid formats</li>
     * </ul>
     */
    @Test
    @DisplayName("valid values, should return valid formats")
    void validValues_shouldReturnValidFormats() {

        Format[] formats = Format.values();
        List<Object> validFormats = validator.validValues("", new HashMap<>());

        for (Format format : formats) {
            assertTrue(
                    validFormats.contains(
                            format.toString())
            );
        }
    }
}
