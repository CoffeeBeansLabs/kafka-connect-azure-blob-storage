package io.coffeebeans.connector.sink.config.recommenders.partitioner.field;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Main purpose of this class is to check should field name configuration be visible or not / is it applicable or not.
 */
public class FieldNameRecommender implements ConfigDef.Recommender {

    @Override
    public List<Object> validValues(String fieldNameConfig, Map<String, Object> parsedConfig) {
        return Collections.emptyList();
    }

    /**
     * Configuration only applicable for Field based partitioning.
     *
     * @param fieldNameConfig Configuration parameter key
     * @param parsedConfig Map of parsed config
     * @return Boolean value based on applicability of the configuration
     */
    @Override
    public boolean visible(String fieldNameConfig, Map<String, Object> parsedConfig) {
        String partitionStrategy = (String) parsedConfig.get(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF_KEY);

        //        return PartitionStrategy.FIELD.toString().equals(partitionStrategy);
        // TODO: Temporary. Make a separate config for timestamp.
        return true;
    }
}
