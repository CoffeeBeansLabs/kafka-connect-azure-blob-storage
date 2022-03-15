package io.coffeebeans.connector.sink.config;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AzureBlobSinkConfigTest {
    private static final String CONN_STR = "AccountName=devstoreaccount1;" +
            "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuF" +
            "q2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProt" +
            "ocol=http;BlobEndpoint=http://host.docker.internal:10000/dev" +
            "storeaccount1;";
    private static final String CONN_STR_INVALID = "http://localhost:1234/";
    private static final String CONTAINER_NAME = "test";
    private static final String TOPIC_DIR = "test";


    /**
     * Should throw a ConfigException for not providing required configuration i.e. connection string
     */
    @Test
    public void test_getConnectionString_withNoConfig() {

        Assertions.assertThrowsExactly(ConfigException.class,
                () -> new AzureBlobSinkConfig(new HashMap<>()),
                "Missing required configuration \"" +
                        AzureBlobSinkConfig.CONN_STRING_CONF + "\" which has no default value."
                );

    }

    /**
     * Should throw a ConfigException for providing invalid connection string
     */
    @Test
    public void test_validateInvalidConnectionString() {
        Map<String, String> parsedConfig = new HashMap<>();
        parsedConfig.put(AzureBlobSinkConfig.CONN_STRING_CONF, CONN_STR_INVALID);
        parsedConfig.put(AzureBlobSinkConfig.CONTAINER_NAME_CONF, CONTAINER_NAME);

        Assertions.assertThrowsExactly(ConfigException.class,
                () -> new AzureBlobSinkConfig(parsedConfig),
                "Invalid connection string: " + CONN_STR_INVALID
                );
    }
}
