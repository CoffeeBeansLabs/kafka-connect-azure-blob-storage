package io.coffeebeans.connector.sink.config.recommenders.partitioner.time;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Recommender for Path format.
 */
public class PathFormatRecommender implements ConfigDef.Recommender {
    @Override
    public List<Object> validValues(String s, Map<String, Object> map) {
        return Collections.emptyList();
    }

    @Override
    public boolean visible(String pathFormatConfigKey, Map<String, Object> parsedConfig) {
        String partitionStrategy = (String) parsedConfig.get(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF_KEY);

        return PartitionStrategy.TIME.toString().equals(partitionStrategy);
    }
}
