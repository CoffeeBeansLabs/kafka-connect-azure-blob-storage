package io.coffeebeans.connector.sink.config.validators;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for TopicsDirValueValidatorTest.
 */
public class TopicsDirValueValidatorTest {
    private static final String TOPICS_DIR_VALUE_NULL = null;
    private static final String TOPICS_DIR_VALUE_EMPTY = "";
    private static final String TOPICS_DIR_VALUE_BLANK = "     ";
    private static final String TOPICS_DIR_VALUE_VALID = "test";

    @Test
    public void shouldThrowExceptionWithNullTopicsDirValue() {
        Assertions.assertThrowsExactly(ConfigException.class,
                () -> new TopicsDirValueValidator().ensureValid(
                        AzureBlobSinkConfig.TOPICS_DIR_CONF_KEY, TOPICS_DIR_VALUE_NULL
        ), "Invalid topics.dir : ");
    }

    @Test
    public void shouldThrowExceptionWithEmptyTopicsDirValue() {
        Assertions.assertThrowsExactly(ConfigException.class,
                () -> new TopicsDirValueValidator().ensureValid(
                        AzureBlobSinkConfig.TOPICS_DIR_CONF_KEY, TOPICS_DIR_VALUE_EMPTY
        ), "Invalid topics.dir : ");
    }

    @Test
    public void shouldThrowExceptionWithBlankTopicsDirValue() {
        Assertions.assertThrowsExactly(ConfigException.class,
                () -> new TopicsDirValueValidator().ensureValid(
                        AzureBlobSinkConfig.TOPICS_DIR_CONF_KEY, TOPICS_DIR_VALUE_BLANK
        ), "Invalid topics.dir : ");
    }

    @Test
    public void shouldNotThrowExceptionWithValidTopicsDirValue() {
        Assertions.assertDoesNotThrow(() -> new TopicsDirValueValidator().ensureValid(
                AzureBlobSinkConfig.TOPICS_DIR_CONF_KEY, TOPICS_DIR_VALUE_VALID
        ));
    }
}
