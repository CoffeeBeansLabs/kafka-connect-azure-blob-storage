package io.coffeebeans.connector.sink.config.recommenders;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PartitionStrategyFieldNameRecommenderTest {

    @Test
    public void isVisibleShouldReturnTrue() {
        Map<String, Object> parsedConfig = new HashMap<>();
        parsedConfig.put(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF, PartitionStrategy.FIELD.toString());

        Assertions.assertTrue(new PartitionStrategyFieldNameRecommender().visible(
                AzureBlobSinkConfig.PARTITION_STRATEGY_FIELD_NAME_CONF, parsedConfig));
    }

    @Test
    public void isVisibleShouldReturnFalse() {
        Map<String, Object> parsedConfig = new HashMap<>();
        parsedConfig.put(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF, PartitionStrategy.TIME.toString());

        Assertions.assertFalse(new PartitionStrategyFieldNameRecommender().visible(
                AzureBlobSinkConfig.PARTITION_STRATEGY_FIELD_NAME_CONF, parsedConfig));
    }
}
