package io.coffeebeans.connector.sink.config;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AzureBlobSinkConfigTest {
    private static final String CONN_STR = "http://localhost:port/";
    private static final String CONTAINER_NAME = "test-container";


    /**
     * Add Connection string property to config map and return it.
     * @return Map of parsed config
     */
    public static Map<String, String> getParsedConfig() {
        Map<String, String> parsedConfig = new HashMap<>();

        parsedConfig.put(AzureBlobSinkConfig.AZURE_BLOB_CONN_STRING_CONF, CONN_STR);
        parsedConfig.put(AzureBlobSinkConfig.AZURE_BLOB_CONTAINER_NAME_CONF, CONTAINER_NAME);
        return parsedConfig;
    }

    /**
     * Should return the provided connection string.
     */
    @Test
    public void test_getConnectionString_withCorrectConfig() {
        AzureBlobSinkConfig azureBlobSinkConfig = new AzureBlobSinkConfig(getParsedConfig());
        Assertions.assertEquals(CONN_STR, azureBlobSinkConfig.getConnectionString());
    }

    /**
     * Should throw a ConfigException for not providing required configuration i.e. connection string
     */
    @Test
    public void test_getConnectionString_withNoConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(AzureBlobSinkConfig.AZURE_BLOB_CONTAINER_NAME_CONF, CONTAINER_NAME);

        Assertions.assertThrowsExactly(ConfigException.class,
                () -> { AzureBlobSinkConfig azureBlobSinkConfig = new AzureBlobSinkConfig(config); },
                "Missing required configuration \"" +
                        AzureBlobSinkConfig.AZURE_BLOB_CONN_STRING_CONF + "\" which has no default value."
                );

    }
}
