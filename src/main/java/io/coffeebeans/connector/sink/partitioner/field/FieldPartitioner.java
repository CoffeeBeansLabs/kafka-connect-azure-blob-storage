package io.coffeebeans.connector.sink.partitioner.field;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connector.sink.partitioner.PartitionerUtil;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * This partitioner will partition the incoming records based on the value of the field specified by the user.
 */
public class FieldPartitioner extends DefaultPartitioner {
    private final String fieldName;

    /**
     * Constructor with Config class object as parameter.
     *
     * @param config AzureBlobSinkConfig config class object
     */
    public FieldPartitioner(AzureBlobSinkConfig config) {
        super(config);

        this.fieldName = config.getFieldName();
        logger.info("Field name configured: {}", fieldName);
    }

    /**
     * I need SinkRecord and starting offset as the parameters. I will extract the value from the field specified by the
     * user and will generate the encoded partition string using that.
     *
     * @param sinkRecord The sink record to be stored
     * @return Encoded partition string
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord) throws JsonProcessingException {
        String fieldValue = PartitionerUtil.getFieldValueAsString(sinkRecord, fieldName);

        /*
          Output format:
          <fieldName>=<fieldValue>
         */
        return fieldName + "=" + fieldValue;
    }
}
