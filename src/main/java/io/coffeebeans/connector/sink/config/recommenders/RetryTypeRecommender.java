package io.coffeebeans.connector.sink.config.recommenders;

import com.azure.storage.common.policy.RetryPolicyType;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigDef;

/**
 * {@link org.apache.kafka.common.config.ConfigDef.Recommender} for `azblob.retry.type` configuration
 */
public class RetryTypeRecommender implements ConfigDef.Recommender {

    /**
     * Returns a list of supported retry policy types by the connector.
     *
     * @param name config key
     * @param parsedConfig Map of user passed configurations
     * @return List of supported retry policy types
     */
    @Override
    public List<Object> validValues(String name, Map<String, Object> parsedConfig) {

        RetryPolicyType[] policies = RetryPolicyType.values();

        return Stream.of(policies)
                .map(RetryPolicyType::name)
                .collect(Collectors.toList());
    }

    /**
     * Always visible.
     *
     * @param name config key
     * @param parsedConfig Map of user passed configurations
     * @return true if visible else false
     */
    @Override
    public boolean visible(String name, Map<String, Object> parsedConfig) {
        return true;
    }
}
