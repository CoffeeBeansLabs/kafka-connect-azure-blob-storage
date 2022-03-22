package io.coffeebeans.connector.sink.partitioner.time;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connector.sink.partitioner.time.extractor.DefaultTimestampExtractor;
import io.coffeebeans.connector.sink.partitioner.time.extractor.RecordFieldTimestampExtractor;
import io.coffeebeans.connector.sink.partitioner.time.extractor.RecordTimestampExtractor;
import io.coffeebeans.connector.sink.partitioner.time.extractor.TimestampExtractor;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A partitioner which will partition the incoming data based on the extracted timestamp.
 */
public class TimeBasedPartitioner extends DefaultPartitioner {
    private TimestampExtractor timestampExtractor;

    @Override
    public void configure(Map<String, String> configProps) {
        super.configure(configProps);

        // Configure timestamp extractor
        this.timestampExtractor
                = getTimestampExtractor(configProps.get(AzureBlobSinkConfig.PARTITION_STRATEGY_TIME_EXTRACTOR_CONF));
        this.timestampExtractor.configure(configProps);
    }

    /**
     * I need the SinkRecord and starting offset as parameters. I will extract the timestamp based on the timestamp
     * extractor strategy configured and will generate the encoded partition string based on that.
     *
     * @param sinkRecord The record to be stored
     * @return Encoded partition string
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord, long startingOffset) {
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
    private TimestampExtractor getTimestampExtractor(String timestampExtractor) {

        switch (timestampExtractor) {
          case "Record": return new RecordTimestampExtractor();
          case "RecordField": return new RecordFieldTimestampExtractor();
          default: return new DefaultTimestampExtractor();
        }
    }
}
