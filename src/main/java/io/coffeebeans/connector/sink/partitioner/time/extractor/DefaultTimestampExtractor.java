package io.coffeebeans.connector.sink.partitioner.time.extractor;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will format the date and time based on the provided path format and system timestamp.
 */
public class DefaultTimestampExtractor implements TimestampExtractor {
    private static final Logger logger = LoggerFactory.getLogger(DefaultTimestampExtractor.class);

    protected String timezone;
    protected String pathFormat;
    protected DateTimeFormatter formatter;

    @Override
    public void configure(Map<String, String> configProps) {
        this.timezone = configProps.get(AzureBlobSinkConfig.PARTITION_STRATEGY_TIME_TIMEZONE_CONF);
        this.pathFormat = configProps.get(AzureBlobSinkConfig.PARTITION_STRATEGY_TIME_PATH_FORMAT_CONF);

        // Initialize the formatter
        this.formatter = DateTimeFormatter.ofPattern(pathFormat);
    }

    /**
     * Get the system date & time and format based on the path format provided.
     *
     * @param sinkRecord The sink record
     * @return Formatted date & time
     */
    @Override
    public String getFormattedTimestamp(SinkRecord sinkRecord) {

        // Get zoned date and time
        ZonedDateTime zonedDateTime = ZonedDateTime.now(ZoneId.of(timezone));

        // Format the zoned date & time
        String formattedTimestamp = formatter.format(zonedDateTime);
        logger.debug("Formatted date & time: {}", formattedTimestamp);

        // Return the formatted date and time string
        return formattedTimestamp;
    }
}
