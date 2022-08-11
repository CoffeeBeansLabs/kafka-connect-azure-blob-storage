package io.coffeebeans.connector.sink.config.recommenders.format;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.FileFormat;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * {@link org.apache.kafka.common.config.ConfigDef.Recommender} for `parquet.codec` configuration
 */
public class ParquetCodecRecommender implements ConfigDef.Recommender {

    /**
     * Returns a list of supported parquet codecs by the connector.
     *
     * @param name configuration name
     * @param parsedConfig Map of user passed configurations
     * @return List of supported file formats
     */
    @Override
    public List<Object> validValues(String name, Map<String, Object> parsedConfig) {

        CompressionCodecName[] supportedCodecs = CompressionCodecName.values();

        return Stream.of(supportedCodecs)
                .map(CompressionCodecName::name)
                .collect(Collectors.toList());
    }

    /**
     * Is this configuration recommended.
     *
     * @param name Conf key
     * @param parsedConfig Map of user passed configurations
     * @return True if recommended else false
     */
    @Override
    public boolean visible(String name, Map<String, Object> parsedConfig) {

        String fileFormat = (String) parsedConfig.get(AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY);

        return FileFormat.PARQUET
                .toString()
                .equalsIgnoreCase(fileFormat);
    }
}
