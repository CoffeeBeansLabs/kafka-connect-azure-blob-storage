package io.coffeebeans.connector.sink.config.recommenders;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PartitionStrategyFieldNameRecommender implements ConfigDef.Recommender {

    @Override
    public List<Object> validValues(String fieldNameConfig, Map<String, Object> parsedConfig) {
        return Collections.emptyList();
    }

    /**
     * Configuration only applicable for Field based partitioning
     * @param fieldNameConfig Configuration parameter
     * @param parsedConfig Map of parsed config
     * @return Boolean value based on applicability of the configuration
     */
    @Override
    public boolean visible(String fieldNameConfig, Map<String, Object> parsedConfig) {
        String partitionStrategy = (String) parsedConfig.get(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF);

        return partitionStrategy != null && partitionStrategy.equals(PartitionStrategy.FIELD.toString());
    }
}
