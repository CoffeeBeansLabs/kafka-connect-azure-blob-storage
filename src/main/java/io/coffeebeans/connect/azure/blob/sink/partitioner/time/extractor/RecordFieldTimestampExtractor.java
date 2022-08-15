package io.coffeebeans.connect.azure.blob.sink.partitioner.time.extractor;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connect.azure.blob.sink.exception.PartitionException;
import io.coffeebeans.connect.azure.blob.sink.partitioner.PartitionerUtil;
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

        this.fieldName = config.getTimestampField();
        log.debug("Field name configured to extract timestamp: {}", fieldName);
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
        long timestamp;
        try {
            timestamp = ((Number) PartitionerUtil.getFieldValue(sinkRecord, fieldName)).longValue();

        } catch (JsonProcessingException e) {
            throw new PartitionException(e);
        }

        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp), ZoneId.of(timezone)
        );

        // Format the zoned date & time
        String formattedTimestamp = formatter.format(zonedDateTime);
        log.debug("Formatted date & time: {}", formattedTimestamp);

        return formattedTimestamp;
    }
}
