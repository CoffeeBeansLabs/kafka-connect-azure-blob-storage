package io.coffeebeans.connector.sink.config.recommenders;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.FileFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Recommender for buffer timeout config parameter.
 */
public class BufferTimeoutRecommender implements ConfigDef.Recommender {
    @Override
    public List<Object> validValues(String bufferTimeoutConfKey, Map<String, Object> parsedConfig) {
        return Collections.emptyList();
    }

    @Override
    public boolean visible(String bufferTimeoutConfKey, Map<String, Object> parsedConfig) {
        String fileFormat = (String) parsedConfig.get(AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY);

        return FileFormat.PARQUET.toString().equals(fileFormat);
    }
}
