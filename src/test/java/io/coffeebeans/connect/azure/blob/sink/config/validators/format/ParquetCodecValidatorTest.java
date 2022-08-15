package io.coffeebeans.connect.azure.blob.sink.config.validators.format;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ParquetCodecValidator}.
 */
public class ParquetCodecValidatorTest {

    private final ParquetCodecValidator validator = new ParquetCodecValidator();

    /**
     * <b>Method: {@link ParquetCodecValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Compression codec is valid</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should not throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given valid codec, should not throw exception")
    void ensureValid_givenValidCodec_shouldNotThrowException() {

        String compressionCodec = "gzip";
        assertDoesNotThrow(() -> validator.ensureValid("", compressionCodec));
    }

    /**
     * <b>Method: {@link ParquetCodecValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Compression codec is invalid</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given invalid codec, should throw exception")
    void ensureValid_givenInvalidCodec_shouldThrowException() {

        String compressionCodec = "invalid";
        assertThrows(ConfigException.class, () -> validator.ensureValid("", compressionCodec));
    }

    /**
     * <b>Method: {@link ParquetCodecValidator#visible(String, Map)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Format: Parquet</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should return true</li>
     * </ul>
     */
    @Test
    @DisplayName("Given parquet format, should return true")
    void visible_givenParquetFormat_shouldReturnTrue() {

        Map<String, Object> parsedConfig = new HashMap<>() {
            {
                put("format", "parquet");
            }
        };
        assertTrue(validator.visible("", parsedConfig));
    }

    /**
     * <b>Method: {@link ParquetCodecValidator#visible(String, Map)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Format: Json</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should return false</li>
     * </ul>
     */
    @Test
    @DisplayName("Given Json format, should return false")
    void visible_givenJsonFormat_shouldReturnFalse() {

        Map<String, Object> parsedConfig = new HashMap<>() {
            {
                put("format", "json");
            }
        };
        assertFalse(validator.visible("", parsedConfig));
    }

    /**
     * <b>Method: {@link ParquetCodecValidator#validValues(String, Map)}</b>.<br>
     * <b>Expectations: </b>
     * <ul>
     *     <li>Should return valid compression codecs</li>
     * </ul>
     */
    @Test
    @DisplayName("Valid values should return Compression codecs")
    void validValues_shouldReturnValidCompressionCodecs() {

        CompressionCodecName[] codecs = CompressionCodecName.values();
        List<Object> validValues = validator.validValues("", new HashMap<>());

        for (CompressionCodecName codec : codecs) {
            assertTrue(
                    validValues.contains(
                            codec.toString())
            );
        }
    }
}
