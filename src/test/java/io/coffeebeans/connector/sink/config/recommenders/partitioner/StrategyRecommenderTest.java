package io.coffeebeans.connector.sink.config.recommenders.partitioner;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for StrategyRecommender.
 */
public class StrategyRecommenderTest {

    @Test
    @DisplayName("Should return valid partition strategies")
    public void shouldReturnValidPartitionStrategies() {
        List<Object> expectedValues = new ArrayList<>();
        Arrays.stream(PartitionStrategy.values()).forEach((strategy) -> expectedValues.add(strategy.toString()));

        List<Object> actualValues = new StrategyRecommender().validValues(
                AzureBlobSinkConfig.PARTITION_STRATEGY_CONF_KEY, new HashMap<>());

        Assertions.assertEquals(expectedValues, actualValues);
    }
}
