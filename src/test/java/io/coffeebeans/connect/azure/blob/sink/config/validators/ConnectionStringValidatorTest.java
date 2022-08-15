package io.coffeebeans.connect.azure.blob.sink.config.validators;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConnectionStringValidator}.
 */
public class ConnectionStringValidatorTest {
    private static final String CONNECTION_URL_NULL = null;
    private static final String CONNECTION_URL_EMPTY = "";
    private static final String CONNECTION_URL_BLANK = "     ";
    private static final String CONNECTION_URL_VALID = "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K"
            + "1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://"
            + "host.docker.internal:10000/devstoreaccount1;";

    private ConnectionStringValidator validator = new ConnectionStringValidator();

    /**
     * <b>Method: {@link ConnectionStringValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumption: </b>
     * <ul>
     *     <li>Connection url is null</li>
     * </ul>
     *
     * <p><b>Expectation: </b>
     * <ul>
     *     <li>Should throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given null connection string, should throw exception")
    void ensureValid_givenNullConnectionString_shouldThrowException() {

        Password connectionString = new Password(CONNECTION_URL_NULL);
        assertThrows(ConfigException.class, () -> validator.ensureValid("", connectionString));
    }

    /**
     * <b>Method: {@link ConnectionStringValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumption: </b>
     * <ul>
     *     <li>Connection url is empty</li>
     * </ul>
     *
     * <p><b>Expectation: </b>
     * <ul>
     *     <li>Should throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given empty connection string, should throw exception")
    void ensureValid_givenEmptyConnectionString_shouldThrowException() {

        Password connectionString = new Password(CONNECTION_URL_EMPTY);
        assertThrows(ConfigException.class, () -> validator.ensureValid("", connectionString));
    }

    /**
     * <b>Method: {@link ConnectionStringValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumption: </b>
     * <ul>
     *     <li>Connection url is blank</li>
     * </ul>
     *
     * <p><b>Expectation: </b>
     * <ul>
     *     <li>Should throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blank connection string, should throw exception")
    void ensureValid_givenBlankConnectionString_shouldThrowException() {

        Password connectionString = new Password(CONNECTION_URL_BLANK);
        assertThrows(ConfigException.class, () -> validator.ensureValid("", connectionString));
    }

    /**
     * <b>Method: {@link ConnectionStringValidator#ensureValid(String, Object)}</b>.<br>
     * <b>Assumption: </b>
     * <ul>
     *     <li>Connection url is valid</li>
     * </ul>
     *
     * <p><b>Expectation: </b>
     * <ul>
     *     <li>Should not throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given valid connection string, should not throw exception")
    void ensureValid_givenValidConnectionString_shouldNotThrowException() {

        Password connectionString = new Password(CONNECTION_URL_VALID);
        assertDoesNotThrow(() -> validator.ensureValid("", connectionString));
    }
}
