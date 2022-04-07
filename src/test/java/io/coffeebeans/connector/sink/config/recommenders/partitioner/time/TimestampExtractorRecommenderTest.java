package io.coffeebeans.connector.sink.config.recommenders.partitioner.time;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.config.recommenders.partitioner.StrategyRecommender;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import io.coffeebeans.connector.sink.partitioner.time.extractor.TimestampExtractorStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class TimestampExtractorRecommenderTest {

    @Test
    @DisplayName("Should return valid timestamp extractor strategies")
    public void shouldReturnValidTimestampExtractorStrategies() {
        List<Object> expectedValues = new ArrayList<>();
        Arrays.stream(TimestampExtractorStrategy.values()).forEach(
                (strategy) -> expectedValues.add(strategy.toString()));

        List<Object> actualValues = new TimestampExtractorRecommender().validValues(
                AzureBlobSinkConfig.PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_CONF_KEY, new HashMap<>());

        Assertions.assertEquals(expectedValues, actualValues);
    }
}
