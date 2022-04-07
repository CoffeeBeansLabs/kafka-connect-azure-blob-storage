package io.coffeebeans.connector.sink.config.recommenders;

import io.coffeebeans.connector.sink.format.FileFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Recommender for file format configuration parameter.
 */
public class FileFormatRecommender implements ConfigDef.Recommender {

    @Override
    public List<Object> validValues(String fileFormatConfKey, Map<String, Object> parsedConfig) {
        List<Object> validValues = new ArrayList<>();
        Arrays.stream(FileFormat.values()).forEach((format) -> validValues.add(format.toString()));

        return validValues;
    }

    @Override
    public boolean visible(String s, Map<String, Object> map) {
        return true;
    }
}
