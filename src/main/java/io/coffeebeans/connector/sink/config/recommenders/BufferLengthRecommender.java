package io.coffeebeans.connector.sink.config.recommenders;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.FileFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Recommender for buffer length config parameter.
 */
public class BufferLengthRecommender implements ConfigDef.Recommender {

    @Override
    public List<Object> validValues(String bufferLengthConfKey, Map<String, Object> parsedConfig) {
        return Collections.emptyList();
    }

    @Override
    public boolean visible(String bufferLengthConfKey, Map<String, Object> parsedConfig) {
        String fileFormat = (String) parsedConfig.get(AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY);

        return FileFormat.PARQUET.toString().equals(fileFormat);
    }
}
