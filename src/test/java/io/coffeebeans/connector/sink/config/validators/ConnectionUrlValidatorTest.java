package io.coffeebeans.connector.sink.config.validators;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ConnectionUrlValidator.
 */
public class ConnectionUrlValidatorTest {
    private static final String CONNECTION_URL_NULL = null;
    private static final String CONNECTION_URL_EMPTY = "";
    private static final String CONNECTION_URL_BLANK = "     ";
    private static final String CONNECTION_URL_VALID = "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K"
            + "1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://"
            + "host.docker.internal:10000/devstoreaccount1;";


    @Test
    public void shouldThrowExceptionWithNullConnectionUrl() {
        Assertions.assertThrowsExactly(ConfigException.class,
                () -> new ConnectionUrlValidator().ensureValid(
                        AzureBlobSinkConfig.CONN_URL_CONF_KEY, CONNECTION_URL_NULL
        ), "Invalid connection string: ");
    }

    @Test
    public void shouldThrowExceptionWithEmptyConnectionUrl() {
        Assertions.assertThrowsExactly(ConfigException.class,
                () -> new ConnectionUrlValidator().ensureValid(
                        AzureBlobSinkConfig.CONN_URL_CONF_KEY, CONNECTION_URL_EMPTY
        ), "Invalid connection string: ");
    }

    @Test
    public void shouldThrowExceptionWithBlankConnectionUrl() {
        Assertions.assertThrowsExactly(ConfigException.class,
                () -> new ConnectionUrlValidator().ensureValid(
                        AzureBlobSinkConfig.CONN_URL_CONF_KEY, CONNECTION_URL_BLANK
        ), "Invalid connection string: ");
    }

    @Test
    public void shouldNotThrowExceptionWithValidConnectionUrl() {
        Assertions.assertDoesNotThrow(() -> new ConnectionUrlValidator().ensureValid(
                AzureBlobSinkConfig.CONN_URL_CONF_KEY, CONNECTION_URL_VALID
        ));
    }
}
