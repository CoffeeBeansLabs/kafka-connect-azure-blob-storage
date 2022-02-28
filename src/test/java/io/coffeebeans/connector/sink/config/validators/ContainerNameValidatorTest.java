package io.coffeebeans.connector.sink.config.validators;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ContainerNameValidatorTest {
    private static final String CONTAINER_NAME_NULL = null;
    private static final String CONTAINER_NAME_EMPTY = "";
    private static final String CONTAINER_NAME_BLANK = "     ";
    private static final String CONTAINER_NAME_VALID = "test";

    @Test
    public void test_nullContainerName() {
        Assertions.assertThrowsExactly(ConfigException.class, () -> {
            new ContainerNameValidator().ensureValid(
                    AzureBlobSinkConfig.CONTAINER_NAME_CONF, CONTAINER_NAME_NULL
            );
        }, "Invalid container name: ");
    }

    @Test
    public void test_emptyContainerName() {
        Assertions.assertThrowsExactly(ConfigException.class, () -> {
            new ContainerNameValidator().ensureValid(
                    AzureBlobSinkConfig.CONTAINER_NAME_CONF, CONTAINER_NAME_EMPTY
            );
        }, "Invalid container name: ");
    }

    @Test
    public void test_blankContainerName() {
        Assertions.assertThrowsExactly(ConfigException.class, () -> {
            new ContainerNameValidator().ensureValid(
                    AzureBlobSinkConfig.CONTAINER_NAME_CONF, CONTAINER_NAME_BLANK
            );
        }, "Invalid container name: ");
    }

    @Test
    public void test_validContainerName() {
        Assertions.assertDoesNotThrow(() -> {
            new ContainerNameValidator().ensureValid(
                    AzureBlobSinkConfig.CONTAINER_NAME_CONF, CONTAINER_NAME_VALID
            );
        });
    }
}
