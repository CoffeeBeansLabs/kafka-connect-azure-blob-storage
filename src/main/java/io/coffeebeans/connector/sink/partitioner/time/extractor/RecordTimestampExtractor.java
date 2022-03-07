package io.coffeebeans.connector.sink.partitioner.time.extractor;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class RecordTimestampExtractor extends DefaultTimestampExtractor {
    private static final Logger logger = LoggerFactory.getLogger(RecordTimestampExtractor.class);

    /**
     * Return the formatted timestamp when the kafka record was produced
     * @param sinkRecord The sink record
     * @return Formatted timestamp
     */
    @Override
    public String getFormattedTimestamp(SinkRecord sinkRecord) {

        // Get the timestamp when the kafka record was produced
        Long timestamp = sinkRecord.timestamp();

        // Get the zoned date & time
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of(timezone));

        // Format the zoned date & time
        String formattedTimestamp = formatter.format(zonedDateTime);
        logger.debug("Formatted date & time: {}", formattedTimestamp);

        return formattedTimestamp;
    }
}
