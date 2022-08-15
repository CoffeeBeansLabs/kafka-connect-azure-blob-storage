package io.coffeebeans.connect.azure.blob.sink.config.validators;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.azure.storage.common.policy.RetryPolicyType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RetryTypeValidator}.
 */
public class RetryTypeValidatorTest {

    private final RetryTypeValidator validator = new RetryTypeValidator();

    /**
     * <b>Method: {@link RetryTypeValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Retry policy type is valid</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should not throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given valid retry policy type, should not throw exception")
    void ensureValid_givenValidPolicyType_shouldNotThrowException() {

        String type = "fixed";
        assertDoesNotThrow(() -> validator.ensureValid("", type));
    }

    /**
     * <b>Method: {@link RetryTypeValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Retry policy type is invalid</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given invalid type, should throw exception")
    void ensureValid_givenInvalidType_shouldThrowException() {

        String type = "invalid";
        assertThrows(ConfigException.class, () -> validator.ensureValid("", type));
    }

    /**
     * <b>Method: {@link RetryTypeValidator#validValues(String, Map)}</b>.<br>
     * <b>Expectations: </b>
     * <ul>
     *     <li>Should return valid compression codecs</li>
     * </ul>
     */
    @Test
    @DisplayName("Valid values should return Compression codecs")
    void validValues_shouldReturnValidCompressionCodecs() {

        RetryPolicyType[] types = RetryPolicyType.values();
        List<Object> validValues = validator.validValues("", new HashMap<>());

        for (RetryPolicyType type : types) {
            assertTrue(
                    validValues.contains(
                            type.toString())
            );
        }
    }
}

