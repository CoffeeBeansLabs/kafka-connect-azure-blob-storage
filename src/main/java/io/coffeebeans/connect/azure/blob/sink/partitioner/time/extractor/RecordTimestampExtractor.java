package io.coffeebeans.connect.azure.blob.sink.partitioner.time.extractor;

import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * This class will extract the time at which the kafka record was produced and will generate the encoded partition
 * string.
 */
public class RecordTimestampExtractor extends DefaultTimestampExtractor {

    /**
     * Constructor.
     *
     * @param config AzureBlobSinkConfig
     */
    public RecordTimestampExtractor(AzureBlobSinkConfig config) {
        super(config);
    }

    /**
     * Return the formatted timestamp when the kafka record was produced.
     *
     * @param sinkRecord The sink record
     * @return Formatted timestamp
     */
    @Override
    public String getFormattedTimestamp(SinkRecord sinkRecord) {

        // Get the timestamp when the kafka record was produced
        Long timestamp = sinkRecord.timestamp();

        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp), ZoneId.of(timezone)
        );

        // Format the zoned date & time
        String formattedTimestamp = formatter.format(zonedDateTime);
        log.debug("Formatted date & time: {}", formattedTimestamp);

        return formattedTimestamp;
    }
}
