package io.coffeebeans.connect.azure.blob.sink.config.validators.format;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.coffeebeans.connect.azure.blob.sink.format.CompressionType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CompressionTypeValidator}.
 */
public class CompressionTypeValidatorTest {

    private final CompressionTypeValidator validator = new CompressionTypeValidator();

    /**
     * <b>Method: {@link CompressionTypeValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Valid compression type</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should not throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given valid compression type, should not throw exception")
    void ensureValid_givenValidType_shouldNotThrowException() {

        String compressionType = "gzip";
        assertDoesNotThrow(() -> validator.ensureValid("", compressionType));
    }

    /**
     * <b>Method: {@link CompressionTypeValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Invalid compression type</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given invalid compression type, should throw exception")
    void ensureValid_givenInvalidType_shouldThrowException() {

        String compressionType = "deflate";
        assertThrows(ConfigException.class, () -> validator.ensureValid("", compressionType));
    }

    /**
     * <b>Method: {@link CompressionTypeValidator#visible(String, Map)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Format: JSON / ByteArray</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should return true</li>
     * </ul>
     */
    @Test
    @DisplayName("Given JSON or ByteArray format, should return true")
    void visible_givenJsonOrByteArrayFormat_shouldReturnTrue() {

        /*
        For JSON format
         */
        Map<String, Object> parsedConfig = new HashMap<>() {
            {
                put("format", "Json");
            }
        };
        assertTrue(validator.visible("", parsedConfig));

        /*
        For ByteArray format
         */
        parsedConfig.put("format", "Bytearray");
        assertTrue(validator.visible("", parsedConfig));
    }

    /**
     * <b>Method: {@link CompressionTypeValidator#visible(String, Map)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Format: Avro</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should return false</li>
     * </ul>
     */
    @Test
    @DisplayName("Given Avro format, should return false")
    void visible_givenAvroFormat_shouldReturnFalse() {

        Map<String, Object> parsedConfig = new HashMap<>() {
            {
                put("format", "Avro");
            }
        };
        assertFalse(validator.visible("", parsedConfig));
    }

    /**
     * <b>Method: {@link CompressionTypeValidator#validValues(String, Map)}</b>.<br>
     * <b>Expectations: </b>
     * <ul>
     *     <li>Should return valid compression types</li>
     * </ul>
     */
    @Test
    @DisplayName("validValues should return valid compression types")
    void validValues_shouldReturnCompressionTypes() {

        CompressionType[] types = CompressionType.values();
        List<Object> validTypes = validator.validValues("", new HashMap<>());

        for (CompressionType type : types) {
            assertTrue(
                    validTypes.contains(
                            type.toString())
            );
        }
    }
}
