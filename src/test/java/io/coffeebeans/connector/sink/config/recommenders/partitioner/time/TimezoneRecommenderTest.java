package io.coffeebeans.connector.sink.config.recommenders.partitioner.time;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for TimezoneRecommender.
 */
public class TimezoneRecommenderTest {

    @Test
    @DisplayName("Configuration property should be visible if partition strategy is TIME")
    public void isVisibleShouldReturnTrue() {
        Map<String, Object> parsedConfig = new HashMap<>();
        parsedConfig.put(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF_KEY, PartitionStrategy.TIME.toString());

        Assertions.assertTrue(new TimezoneRecommender().visible(
                AzureBlobSinkConfig.PARTITION_STRATEGY_TIME_TIMEZONE_CONF_KEY, parsedConfig));
    }

    @Test
    @DisplayName("Configuration property should not be visible if partition strategy is other than TIME")
    public void isVisibleShouldReturnFalse() {
        Map<String, Object> parsedConfig = new HashMap<>();
        parsedConfig.put(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF_KEY, PartitionStrategy.FIELD.toString());

        Assertions.assertFalse(new TimezoneRecommender().visible(
                AzureBlobSinkConfig.PARTITION_STRATEGY_TIME_TIMEZONE_CONF_KEY, parsedConfig));
    }
}
