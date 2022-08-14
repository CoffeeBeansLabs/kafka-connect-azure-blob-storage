package io.coffeebeans.connector.sink.config.validators.format;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.CompressionType;
import io.coffeebeans.connector.sink.format.FileFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * {@link org.apache.kafka.common.config.ConfigDef.Validator} and
 * {@link org.apache.kafka.common.config.ConfigDef.Recommender} for
 * {@link io.coffeebeans.connector.sink.config.AzureBlobSinkConfig#COMPRESSION_TYPE_CONF az.compression.type}
 * config.
 */
public class CompressionTypeValidator implements ConfigDef.Validator, ConfigDef.Recommender {

    /**
     * The valid values for the configuration given the current configuration values.
     *
     * @param name         The name of the configuration
     * @param parsedConfig The parsed configuration values
     * @return The list of valid values. To function properly, the returned objects should have the type
     * defined for the configuration using the recommender.
     */
    @Override
    public List<Object> validValues(String name, Map<String, Object> parsedConfig) {

        CompressionType[] types = CompressionType.values();

        return Stream.of(types)
                .map(CompressionType::name)
                .collect(Collectors.toList());
    }

    /**
     * Set the visibility of the configuration given the current configuration values.
     * {@link io.coffeebeans.connector.sink.config.AzureBlobSinkConfig#COMPRESSION_TYPE_CONF az.compression.type}
     * is only valid when
     * {@link io.coffeebeans.connector.sink.config.AzureBlobSinkConfig#FILE_FORMAT_CONF_KEY file.format}
     * is either JSON or BYTEARRAY.
     *
     * @param name         The name of the configuration
     * @param parsedConfig The parsed configuration values
     * @return The visibility of the configuration
     */
    @Override
    public boolean visible(String name, Map<String, Object> parsedConfig) {

        String fileFormat = (String) parsedConfig.get(
                AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY
        );
        boolean isJson = FileFormat.JSON
                .toString()
                .equalsIgnoreCase(fileFormat);

        boolean isByteArray = FileFormat.BYTEARRAY
                .toString()
                .equalsIgnoreCase(fileFormat);

        return isJson || isByteArray;
    }

    /**
     * Perform single configuration validation.
     *
     * @param name  The name of the configuration
     * @param value The value of the configuration
     * @throws ConfigException if the value is invalid.
     */
    @Override
    public void ensureValid(String name, Object value) throws ConfigException {

        String compressionType = (String) value;

        for (CompressionType type : CompressionType.values()) {
            if (type.toString().equalsIgnoreCase(compressionType)) {
                return;
            }
        }
        throw new ConfigException(name, value, "Check list of supported of compression types");
    }
}
