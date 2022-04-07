package io.coffeebeans.connector.sink.config.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * This class will validate <code>topics.dir</code> value provided by the user as configuration parameter.
 */
public class TopicsDirValueValidator implements ConfigDef.Validator {

    /**
     * Ensure the provided value is not null, empty and blank.
     *
     * @param topicsDirConfigKey topics.dir config key
     * @param topicsDirValue topics.dir value
     */
    @Override
    public void ensureValid(String topicsDirConfigKey, Object topicsDirValue) {
        String topicsDir = (String) topicsDirValue;

        if (topicsDir == null || topicsDir.isEmpty() || topicsDir.isBlank()) {
            throw new ConfigException("Invalid topics.dir : ", topicsDir);
        }
    }
}
