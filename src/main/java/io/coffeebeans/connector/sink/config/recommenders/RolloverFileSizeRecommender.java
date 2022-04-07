package io.coffeebeans.connector.sink.config.recommenders;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.FileFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Recommender for rollover file size config parameter.
 */
public class RolloverFileSizeRecommender implements ConfigDef.Recommender {

    @Override
    public List<Object> validValues(String s, Map<String, Object> map) {
        return Collections.emptyList();
    }

    @Override
    public boolean visible(String rolloverFilePolicySizeConfKey, Map<String, Object> parsedConfig) {
        String fileFormat = (String) parsedConfig.get(AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY);

        return FileFormat.NONE.toString().equals(fileFormat);
    }
}
