package io.coffeebeans.connector.sink.config.recommenders.format;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.FileFormat;
import io.coffeebeans.connector.sink.format.avro.SupportedAvroCodecs;
import org.apache.kafka.common.config.ConfigDef;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link ConfigDef.Recommender} for `avro.codec` configuration
 */
public class AvroCodecRecommender implements ConfigDef.Recommender {

    /**
     * Returns a list of supported avro codecs by the connector.
     *
     * @param name configuration name
     * @param parsedConfig Map of user passed configurations
     * @return List of supported codecs
     */
    @Override
    public List<Object> validValues(String name, Map<String, Object> parsedConfig) {

        SupportedAvroCodecs[] supportedAvroCodecs = SupportedAvroCodecs.values();

        return Stream.of(supportedAvroCodecs)
                .map(SupportedAvroCodecs::name)
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

        return FileFormat.AVRO
                .toString()
                .equalsIgnoreCase(fileFormat);
    }
}
