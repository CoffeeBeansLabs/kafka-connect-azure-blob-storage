package io.coffeebeans.connector.sink.config.recommenders.partitioner.time;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import io.coffeebeans.connector.sink.partitioner.time.extractor.TimestampExtractorStrategy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Recommender for timestamp extractor configuration.
 */
public class TimestampExtractorRecommender implements ConfigDef.Recommender {

    @Override
    public List<Object> validValues(String timestampExtractorConfKey, Map<String, Object> parsedConfig) {
        List<Object> validValues = new ArrayList<>();
        Arrays.stream(TimestampExtractorStrategy.values()).forEach((value) -> validValues.add(value.toString()));

        return validValues;
    }

    @Override
    public boolean visible(String timestampExtractorConfKey, Map<String, Object> parsedConfig) {
        String partitionStrategy = (String) parsedConfig.get(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF_KEY);

        return PartitionStrategy.TIME.toString().equals(partitionStrategy);
    }
}
