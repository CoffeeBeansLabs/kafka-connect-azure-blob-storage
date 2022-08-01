package io.coffeebeans.connector.sink.config.recommenders.partitioner.time;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.time.extractor.TimestampExtractorStrategy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for TimestampExtractorRecommender.
 */
public class TimestampExtractorRecommenderTest {

    @Test
    @DisplayName("Given config, validValues method should return supported timestamp extractor strategies")
    void validValues_givenConfigProps_shouldReturnSupportedTimestampExtractorStrategies() {

        TimestampExtractorStrategy[] supportedExtractorStrategies = TimestampExtractorStrategy.values();

        List<Object> expectedValues = Stream.of(supportedExtractorStrategies)
                .map(TimestampExtractorStrategy::name)
                .collect(Collectors.toList());

        List<Object> actualValues = new TimestampExtractorRecommender().validValues(
                AzureBlobSinkConfig.PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_CONF_KEY, new HashMap<>()
        );

        Assertions.assertEquals(expectedValues, actualValues);
    }

    @Test
    @DisplayName("Given config, visible method should return true when partition strategy is set to TIME")
    void visible_givenConfigProps_whenPartitionStrategyIsTime_shouldReturnTrue() {

        String partitionStrategy = "TIME";

        Map<String, Object> configProps = new HashMap<>() {
            {
                put(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF_KEY, partitionStrategy);
            }
        };

        boolean isVisible = new TimestampExtractorRecommender().visible(
                AzureBlobSinkConfig.PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_CONF_KEY, configProps
        );

        Assertions.assertTrue(isVisible);
    }

    @Test
    @DisplayName("Given config, visible method should return false when partition strategy is set to FIELD")
    void visible_givenConfigProps_whenPartitionStrategyIsField_shouldReturnFalse() {

        String partitionStrategy = "FIELD";

        Map<String, Object> configProps = new HashMap<>() {
            {
                put(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF_KEY, partitionStrategy);
            }
        };

        boolean isVisible = new TimestampExtractorRecommender().visible(
                AzureBlobSinkConfig.PARTITION_STRATEGY_TIME_TIMESTAMP_EXTRACTOR_CONF_KEY, configProps
        );

        Assertions.assertFalse(isVisible);
    }
}
