package io.coffeebeans.connect.azure.blob.sink.partitioner.time;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connect.azure.blob.sink.exception.PartitionException;
import io.coffeebeans.connect.azure.blob.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connect.azure.blob.sink.partitioner.time.extractor.DefaultTimestampExtractor;
import io.coffeebeans.connect.azure.blob.sink.partitioner.time.extractor.RecordFieldTimestampExtractor;
import io.coffeebeans.connect.azure.blob.sink.partitioner.time.extractor.RecordTimestampExtractor;
import io.coffeebeans.connect.azure.blob.sink.partitioner.time.extractor.TimestampExtractor;
import io.coffeebeans.connect.azure.blob.sink.partitioner.time.extractor.TimestampExtractorStrategy;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A partitioner which will partition the incoming data based on the extracted timestamp.
 */
public class TimePartitioner extends DefaultPartitioner {
    private final TimestampExtractor timestampExtractor;

    /**
     * Constructor.
     *
     * @param config AzureBlobSinkConfig
     */
    public TimePartitioner(AzureBlobSinkConfig config) {
        super(config);

        this.timestampExtractor = getTimestampExtractor(config.getTimestampExtractor(), config);
    }

    /**
     * Generate the encoded partition based on the timestamp value.
     * The timestamp value is extracted either from the record,
     * server or record producing time based on the extractor strategy configured.
     *
     * <p>&lt;formattedTimestamp&gt;
     *
     * @param sinkRecord The record to be stored
     * @return Encoded partition string
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord) throws PartitionException {
        try {
            return timestampExtractor.getFormattedTimestamp(sinkRecord);

        } catch (JsonProcessingException e) {
            throw new PartitionException(e);
        }
    }

    /**
     * It returns the TimestampExtractor based on the provided configuration.
     *
     * @param timestampExtractor type of timestamp extractor
     * @return TimestampExtractor
     */
    private TimestampExtractor getTimestampExtractor(String timestampExtractor, AzureBlobSinkConfig config) {

        TimestampExtractorStrategy strategy = TimestampExtractorStrategy.valueOf(timestampExtractor);
        log.debug("Timestamp extractor strategy configured: {}", strategy);

        switch (strategy) {
            case  RECORD: return new RecordTimestampExtractor(config);
            case RECORD_FIELD: return new RecordFieldTimestampExtractor(config);
            default: return new DefaultTimestampExtractor(config);
        }
    }
}
