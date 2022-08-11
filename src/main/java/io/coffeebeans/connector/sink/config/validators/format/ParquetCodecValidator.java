package io.coffeebeans.connector.sink.config.validators.format;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * {@link ConfigDef.Validator} for `parquet.codec` configuration
 */
public class ParquetCodecValidator implements ConfigDef.Validator {

    /**
     * Perform single configuration validation.
     *
     * @param name  The name of the configuration
     * @param value The value of the configuration
     */
    @Override
    public void ensureValid(String name, Object value) {

        String configuredParquetCodec = (String) value;

        for (CompressionCodecName codec : CompressionCodecName.values()) {
            if (codec.toString().equalsIgnoreCase(configuredParquetCodec)) {
                return;
            }
        }
        throw new ConfigException("Invalid compression codec");
    }
}
