package io.coffeebeans.connector.sink.config.recommenders;

import io.coffeebeans.connector.sink.format.FileFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigDef;

/**
 * Recommender for file format configuration parameter.
 */
public class FileFormatRecommender implements ConfigDef.Recommender {

    /**
     * Returns a list of supported file formats by the connector.
     *
     * @param fileFormatConf file format conf key
     * @param configProps Map of user passed configurations
     * @return List of supported file formats
     */
    @Override
    public List<Object> validValues(String fileFormatConf, Map<String, Object> configProps) {

        FileFormat[] supportedFileFormats = FileFormat.values();

        return Stream.of(supportedFileFormats)
                .map(FileFormat::name)
                .collect(Collectors.toList());
    }

    /**
     * Is this configuration recommended.
     *
     * @param s Conf key
     * @param map Map of user passed configurations
     * @return True if recommended else false
     */
    @Override
    public boolean visible(String s, Map<String, Object> map) {
        return true;
    }
}
