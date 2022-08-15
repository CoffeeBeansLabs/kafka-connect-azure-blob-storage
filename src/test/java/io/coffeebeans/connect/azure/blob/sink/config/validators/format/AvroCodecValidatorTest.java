package io.coffeebeans.connect.azure.blob.sink.config.validators.format;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.coffeebeans.connect.azure.blob.sink.format.avro.AvroCodecs;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AvroCodecValidator}.
 */
public class AvroCodecValidatorTest {

    private final AvroCodecValidator validator = new AvroCodecValidator();

    /**
     * <b>Method: {@link AvroCodecValidator#ensureValid(String, Object)}</b>.<br>
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

        String compressionCodec = "bzip2";
        assertDoesNotThrow(() -> validator.ensureValid("", compressionCodec));
    }

    /**
     * <b>Method: {@link AvroCodecValidator#ensureValid(String, Object)}</b>.<br>
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
     * <b>Method: {@link AvroCodecValidator#visible(String, Map)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Format: Avro</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should return true</li>
     * </ul>
     */
    @Test
    @DisplayName("Given Avro format, should return true")
    void visible_givenAvroFormat_shouldReturnTrue() {

        Map<String, Object> parsedConfig = new HashMap<>() {
            {
                put("format", "avro");
            }
        };
        assertTrue(validator.visible("", parsedConfig));
    }

    /**
     * <b>Method: {@link AvroCodecValidator#visible(String, Map)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Format: Parquet</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should return false</li>
     * </ul>
     */
    @Test
    @DisplayName("Given Parquet format, should return false")
    void visible_givenParquetFormat_shouldReturnFalse() {

        Map<String, Object> parsedConfig = new HashMap<>() {
            {
                put("format", "parquet");
            }
        };
        assertFalse(validator.visible("", parsedConfig));
    }

    /**
     * <b>Method: {@link AvroCodecValidator#validValues(String, Map)}</b>.<br>
     * <b>Expectations: </b>
     * <ul>
     *     <li>Should return Avro compression codecs</li>
     * </ul>
     */
    @Test
    @DisplayName("Valid values should return Avro codecs")
    void validValues_shouldReturnAvroCodecs() {

        AvroCodecs[] avroCodecs = AvroCodecs.values();
        List<Object> validValues = validator.validValues("", new HashMap<>());

        for (AvroCodecs codec : avroCodecs) {
            assertTrue(
                    validValues.contains(
                            codec.toString())
            );
        }
    }
}
