package io.coffeebeans.connector.sink.partitioner.time;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connector.sink.partitioner.time.extractor.DefaultTimestampExtractor;
import io.coffeebeans.connector.sink.partitioner.time.extractor.RecordFieldTimestampExtractor;
import io.coffeebeans.connector.sink.partitioner.time.extractor.RecordTimestampExtractor;
import io.coffeebeans.connector.sink.partitioner.time.extractor.TimestampExtractor;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;


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
     * @param sinkRecord The record to be stored
     * @return <prefix>/<kafkaTopic>/<formattedTimestamp>/<kafkaTopic>+<kafkaPartition>+<startOffset>.<format>
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord, long startingOffset) {
        String kafkaTopic = sinkRecord.topic();
        long kafkaPartition = sinkRecord.kafkaPartition();

        return prefix + FOLDER_DELIMITER + // <prefix>/

                // <topic>/
                kafkaTopic + FOLDER_DELIMITER +

                // <formattedTimestamp>/
                timestampExtractor.getFormattedTimestamp(sinkRecord) + FOLDER_DELIMITER +

                // <kafkaTopic> + <kafkaPartition> + <startOffset>
                kafkaTopic + FILE_DELIMITER + kafkaPartition + FILE_DELIMITER + startingOffset;
    }

    /**
     * It returns the TimestampExtractor based on the provided configuration
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
