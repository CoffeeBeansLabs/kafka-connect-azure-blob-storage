package io.coffeebeans.connector.sink.config.recommenders.partitioner.field;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class FieldNameRecommenderTest {

    @Test
    @DisplayName("Configuration property should be visible if partition strategy is FIELD")
    public void isVisibleShouldReturnTrue() {
        Map<String, Object> parsedConfig = new HashMap<>();
        parsedConfig.put(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF_KEY, PartitionStrategy.FIELD.toString());

        Assertions.assertTrue(new FieldNameRecommender().visible(
                AzureBlobSinkConfig.PARTITION_STRATEGY_FIELD_NAME_CONF_KEY, parsedConfig));
    }

    // TODO: Temporarily it's true until separate config is declared for RECORD_FIELD timestamp extractor.
    @Test
    @DisplayName("Configuration property should be visible if partition strategy is other than FIELD")
    public void isVisibleShouldReturnFalse() {
        Map<String, Object> parsedConfig = new HashMap<>();
        parsedConfig.put(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF_KEY, PartitionStrategy.TIME.toString());

        Assertions.assertTrue(new FieldNameRecommender().visible(
                AzureBlobSinkConfig.PARTITION_STRATEGY_FIELD_NAME_CONF_KEY, parsedConfig));
    }
}
