package io.coffeebeans.connector.sink.config.recommenders.partitioner.time;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import io.coffeebeans.connector.sink.partitioner.time.extractor.TimestampExtractorStrategy;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Recommender for timestamp extractor configuration.
 */
public class TimestampExtractorRecommender implements ConfigDef.Recommender {

    /**
     * Returns supported timestamp extractor strategies.
     *
     * @param timestampExtractorConf TimestampExtractor configuration key
     * @param configProps Map of user defined configuration
     * @return List of supported timestamp strategies
     */
    @Override
    public List<Object> validValues(String timestampExtractorConf, Map<String, Object> configProps) {

        TimestampExtractorStrategy[] supportedExtractorStrategies = TimestampExtractorStrategy.values();

        return Stream.of(supportedExtractorStrategies)
                .map(TimestampExtractorStrategy::name)
                .collect(Collectors.toList());
    }

    /**
     * Returns whether this configuration is recommended or not.
     * Timestamp extractor strategy configuration is only valid if partition strategy is
     * configured to TIME.
     *
     * @param timestampExtractorConf TimestampExtractor configuration key
     * @param configProps Map of user defined configuration
     * @return True if recommended or else false
     */
    @Override
    public boolean visible(String timestampExtractorConf, Map<String, Object> configProps) {
        String partitionStrategy = (String) configProps.get(AzureBlobSinkConfig.PARTITION_STRATEGY_CONF_KEY);

        return PartitionStrategy.TIME.toString().equalsIgnoreCase(partitionStrategy);
    }
}
