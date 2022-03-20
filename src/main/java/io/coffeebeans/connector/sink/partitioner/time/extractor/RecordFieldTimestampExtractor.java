package io.coffeebeans.connector.sink.partitioner.time.extractor;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.PartitionerUtil;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

public class RecordFieldTimestampExtractor extends DefaultTimestampExtractor {
    private static final Logger logger = LoggerFactory.getLogger(RecordFieldTimestampExtractor.class);

    private String field;

    @Override
    public void configure(Map<String, String> configProps) {
        super.configure(configProps);
        this.field = configProps.get(AzureBlobSinkConfig.PARTITION_STRATEGY_FIELD_NAME_CONF);
    }

    /**
     * Extract the timestamp from the field value and format it the pathFormat
     * @param sinkRecord The sink record
     * @return Formatted date & time
     */
    @Override
    public String getFormattedTimestamp(SinkRecord sinkRecord) {

        // Extract timestamp from the field value
        long timestamp = ((Number) PartitionerUtil.getFieldValue(sinkRecord, field)).longValue();

        // Get the zoned date & time
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of(timezone));

        // Format the zoned date & time
        String formattedTimestamp = formatter.format(zonedDateTime);
        logger.debug("Formatted date & time: {}", formattedTimestamp);

        return formattedTimestamp;
    }
}
