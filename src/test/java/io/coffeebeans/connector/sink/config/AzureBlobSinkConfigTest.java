package io.coffeebeans.connector.sink.config;

import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Unit tests for AzureBlobSinkConfig.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AzureBlobSinkConfigTest {
    private static final String CONN_STR_VALUE = "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuF"
            + "q2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProt"
            + "ocol=http;BlobEndpoint=http://host.docker.internal:10000/dev"
            + "storeaccount1;";

    private Map<String, String> configProps;

    @BeforeAll
    public void init() {
        configProps = new HashMap<>();
    }

    @Test
    @Order(1)
    @DisplayName("Should fail if connection url is not configured")
    public void shouldFailIfConnectionUrlNotPassed() {

        Assertions.assertThrowsExactly(ConfigException.class,
                () -> new AzureBlobSinkConfig(this.configProps),
                "Missing required configuration \""
                        + AzureBlobSinkConfig.CONN_URL_CONF_KEY + "\" which has no default value."
        );

        // Add connection string for next unit test.
        this.configProps.put(AzureBlobSinkConfig.CONN_URL_CONF_KEY, CONN_STR_VALUE);
    }

    @Test
    @Order(2)
    @DisplayName("Should fail if field name is not configured for FIELD based partition strategy")
    public void shouldFailIfFieldNameNotProvidedInFieldPartitionStrategy() {
        this.configProps.put(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF_KEY, PartitionStrategy.FIELD.toString());

        Assertions.assertThrowsExactly(ConfigException.class,
                () -> new AzureBlobSinkConfig(this.configProps),
                "Missing required configuration \""
                        + AzureBlobSinkConfig.PARTITION_STRATEGY_FIELD_NAME_CONF_KEY + "\" which has no default value."
        );
    }
}
