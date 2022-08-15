package io.coffeebeans.connect.azure.blob.sink;

import org.apache.kafka.connect.connector.Connector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AzureBlobSinkConnector}.
 */
public class AzureBlobSinkConnectorTest {

    @Test
    public void version() {
        String version = new AzureBlobSinkConnector().version();
        Assertions.assertNotNull(version);
        Assertions.assertFalse(version.isEmpty());
    }

    @Test
    public void connectorType() {
        Connector connector = new AzureBlobSinkConnector();
        Assertions.assertTrue(AzureBlobSinkConnector.class.isAssignableFrom(connector.getClass()));
    }
}
