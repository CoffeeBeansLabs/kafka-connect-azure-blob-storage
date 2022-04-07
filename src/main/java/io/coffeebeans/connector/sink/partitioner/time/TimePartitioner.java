package io.coffeebeans.connector.sink.partitioner.time;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connector.sink.partitioner.time.extractor.DefaultTimestampExtractor;
import io.coffeebeans.connector.sink.partitioner.time.extractor.RecordFieldTimestampExtractor;
import io.coffeebeans.connector.sink.partitioner.time.extractor.RecordTimestampExtractor;
import io.coffeebeans.connector.sink.partitioner.time.extractor.TimestampExtractor;
import io.coffeebeans.connector.sink.partitioner.time.extractor.TimestampExtractorStrategy;
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

        this.timestampExtractor = getTimestampExtractor(config.getTimeExtractor(), config);
    }

    /**
     * I need the SinkRecord and starting offset as parameters. I will extract the timestamp based on the timestamp
     * extractor strategy configured and will generate the encoded partition string based on that.
     *
     * @param sinkRecord The record to be stored
     * @return Encoded partition string
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        /*
          Output format:
          <formattedTimestamp>
         */
        return timestampExtractor.getFormattedTimestamp(sinkRecord);
    }

    /**
     * It returns the TimestampExtractor based on the provided configuration.
     *
     * @param timestampExtractor type of timestamp extractor
     * @return TimestampExtractor
     */
    private TimestampExtractor getTimestampExtractor(String timestampExtractor, AzureBlobSinkConfig config) {
        TimestampExtractorStrategy strategy = TimestampExtractorStrategy.valueOf(timestampExtractor);
        logger.info("Timestamp extractor strategy configured: {}", strategy);

        switch (strategy) {
          case  RECORD: return new RecordTimestampExtractor(config);
          case RECORD_FIELD: return new RecordFieldTimestampExtractor(config);
          default: return new DefaultTimestampExtractor(config);
        }
    }
}
