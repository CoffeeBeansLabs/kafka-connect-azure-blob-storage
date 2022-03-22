package io.coffeebeans.connector.sink.partitioner.field;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connector.sink.partitioner.PartitionerUtil;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This partitioner will partition the incoming records based on the value of the field specified by the user.
 */
public class FieldPartitioner extends DefaultPartitioner {
    private static final Logger logger = LoggerFactory.getLogger(FieldPartitioner.class);

    private String fieldName;

    @Override
    public void configure(Map<String, String> configProps) {
        super.configure(configProps);
        this.fieldName = configProps.get(AzureBlobSinkConfig.PARTITION_STRATEGY_FIELD_NAME_CONF);
    }

    /**
     * I need SinkRecord and starting offset as the parameters. I will extract the value from the field specified by the
     * user and will generate the encoded partition string using that.
     *
     * @param sinkRecord The sink record to be stored
     * @param startingOffset kafka offset of the first record of the batch
     * @return Encoded partition string
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord, long startingOffset) {
        String fieldValue = PartitionerUtil.getFieldValueAsString(sinkRecord, fieldName);

        /*
          Output format:
          <fieldName>=<fieldValue>
         */
        return fieldName + "=" + fieldValue;
    }
}
