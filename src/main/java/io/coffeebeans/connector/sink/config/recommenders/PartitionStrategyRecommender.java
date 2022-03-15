package io.coffeebeans.connector.sink.config.recommenders;

import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PartitionStrategyRecommender implements ConfigDef.Recommender {

    @Override
    public List<Object> validValues(String partitionStrategyConf, Map<String, Object> configProps) {
        List<Object> validValues = new ArrayList<>();

        for (PartitionStrategy strategy : PartitionStrategy.values()) {
            validValues.add(strategy.toString());
        }
        return validValues;
    }

    @Override
    public boolean visible(String partitionStrategyConf, Map<String, Object> configProps) {
        return true;
    }
}
