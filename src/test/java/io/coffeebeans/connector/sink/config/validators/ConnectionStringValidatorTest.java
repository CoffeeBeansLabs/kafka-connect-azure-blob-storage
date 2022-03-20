package io.coffeebeans.connector.sink.config.validators;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectionStringValidatorTest {
    private static final String CONNECTION_STRING_NULL = null;
    private static final String CONNECTION_STRING_EMPTY = "";
    private static final String CONNECTION_STRING_BLANK = "     " ;
    private static final String CONNECTION_STRING_VALID = "AccountName=devstoreaccount1;" +
            "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K" +
            "1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://" +
            "host.docker.internal:10000/devstoreaccount1;";


    @Test
    public void test_nullConnectionString() {
        Assertions.assertThrowsExactly(ConfigException.class, () -> {
            new ConnectionStringValidator().ensureValid(
                    AzureBlobSinkConfig.CONN_URL_CONF_KEY, CONNECTION_STRING_NULL
            );
        }, "Invalid connection string: ");
    }

    @Test
    public void test_emptyConnectionString() {
        Assertions.assertThrowsExactly(ConfigException.class, () -> {
            new ConnectionStringValidator().ensureValid(
                    AzureBlobSinkConfig.CONN_URL_CONF_KEY, CONNECTION_STRING_EMPTY
            );
        }, "Invalid connection string: ");
    }

    @Test
    public void test_blankConnectionString() {
        Assertions.assertThrowsExactly(ConfigException.class, () -> {
            new ConnectionStringValidator().ensureValid(
                    AzureBlobSinkConfig.CONN_URL_CONF_KEY, CONNECTION_STRING_BLANK
            );
        }, "Invalid connection string: ");
    }

    @Test
    public void test_validConnectionString() {
        Assertions.assertDoesNotThrow(() -> {
            new ConnectionStringValidator().ensureValid(
                    AzureBlobSinkConfig.CONN_URL_CONF_KEY, CONNECTION_STRING_VALID
            );
        });
    }
}
