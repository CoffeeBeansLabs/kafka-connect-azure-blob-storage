package io.coffeebeans.connector.sink.partitioner.time.extractor;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.PartitionerUtil;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.apache.kafka.connect.sink.SinkRecord;


/**
 * This timestamp extractor will extract the timestamp from the value of the field specified by the user and will
 * generate the encoded partition string based on that.
 */
public class RecordFieldTimestampExtractor extends DefaultTimestampExtractor {
    private final String fieldName;

    /**
     * Constructor.
     *
     * @param config AzureBlobSinkConfig
     */
    public RecordFieldTimestampExtractor(AzureBlobSinkConfig config) {
        super(config);

        // TODO: create separate field name config for record-field timestamp extractor
        this.fieldName = config.getFieldName();
        logger.info("Field name configured to extract timestamp: {}", fieldName);
    }

    /**
     * Extract the timestamp from the field value and format it the pathFormat.
     *
     * @param sinkRecord The sink record
     * @return Formatted date & time
     */
    @Override
    public String getFormattedTimestamp(SinkRecord sinkRecord) {

        // Extract timestamp from the field value
        long timestamp = ((Number) PartitionerUtil.getFieldValue(sinkRecord, fieldName)).longValue();

        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of(timezone));

        // Format the zoned date & time
        String formattedTimestamp = formatter.format(zonedDateTime);
        logger.debug("Formatted date & time: {}", formattedTimestamp);

        return formattedTimestamp;
    }
}
