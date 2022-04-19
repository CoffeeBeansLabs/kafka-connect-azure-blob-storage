package io.coffeebeans.connector.sink.partitioner.time.extractor;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will format the date and time based on the provided path format and system timestamp.
 */
public class DefaultTimestampExtractor implements TimestampExtractor {
    protected static final Logger logger = LoggerFactory.getLogger(TimestampExtractor.class);

    protected String timezone;
    protected String pathFormat;
    protected DateTimeFormatter formatter;

    /**
     * Constructor which takes the Config class.
     *
     * @param config AzureBlobSinkConfig class object containing all the configuratino parameters
     */
    public DefaultTimestampExtractor(AzureBlobSinkConfig config) {
        this.timezone = config.getTimezone();
        this.pathFormat = config.getPathFormat();

        // Initialize the formatter
        this.formatter = DateTimeFormatter.ofPattern(pathFormat);
        logger.info("Time partitioner path format configured: {}", pathFormat);
    }

    /**
     * Get the system date & time and format based on the path format provided.
     *
     * @param sinkRecord The sink record
     * @return Formatted date & time
     */
    @Override
    public String getFormattedTimestamp(SinkRecord sinkRecord) throws JsonProcessingException {
        ZonedDateTime zonedDateTime = ZonedDateTime.now(ZoneId.of(timezone));

        // Format the zoned date & time
        String formattedTimestamp = formatter.format(zonedDateTime);
        logger.debug("Formatted date & time: {}", formattedTimestamp);

        return formattedTimestamp;
    }
}
