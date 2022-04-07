package io.coffeebeans.connector.sink.config.recommenders.partitioner;

import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * This recommender will recommend valid values for the partition strategy configuration.
 */
public class StrategyRecommender implements ConfigDef.Recommender {

    @Override
    public List<Object> validValues(String partitionStrategyConf, Map<String, Object> configProps) {
        List<Object> validValues = new ArrayList<>();
        Arrays.stream(PartitionStrategy.values()).forEach((value) -> validValues.add(value.toString()));

        return validValues;
    }

    @Override
    public boolean visible(String partitionStrategyConf, Map<String, Object> configProps) {
        return true;
    }
}
