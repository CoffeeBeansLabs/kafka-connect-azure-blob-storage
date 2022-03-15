package io.coffeebeans.connector.sink.config.recommenders;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PartitionStrategyRecommenderTest {

    @Test
    public void shouldReturnValidPartitionStrategies() {
        List<Object> expectedValues = new ArrayList<>();
        for (PartitionStrategy strategy : PartitionStrategy.values()) {
            expectedValues.add(strategy.toString());
        }

        PartitionStrategyRecommender partitionStrategyRecommender = new PartitionStrategyRecommender();
        List<Object> actualValues = partitionStrategyRecommender.validValues(
                AzureBlobSinkConfig.PARTITION_STRATEGY_CONF, new HashMap<>());

        Assertions.assertEquals(expectedValues, actualValues);
        Assertions.assertTrue(partitionStrategyRecommender.visible(
                AzureBlobSinkConfig.PARTITION_STRATEGY_CONF, new HashMap<>()));
    }
}
