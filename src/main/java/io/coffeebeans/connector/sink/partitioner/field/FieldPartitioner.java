package io.coffeebeans.connector.sink.partitioner.field;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connector.sink.partitioner.PartitionerUtil;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * This partitioner will partition the incoming records based on the value of the field specified.
 */
public class FieldPartitioner extends DefaultPartitioner {
    private final String fieldName;

    /**
     * Constructor.
     *
     * @param config AzureBlobSinkConfig config class object
     */
    public FieldPartitioner(AzureBlobSinkConfig config) {
        super(config);

        fieldName = config.getFieldName();
        log.info("Field name configured: {}", fieldName);
    }

    /**
     * Generate the encoded partition string by extracting the value of the
     * specified field.
     *
     * <p>&lt;fieldName&gt;=&lt;fieldValue&gt;
     *
     * @param sinkRecord The sink record to be stored
     * @return Encoded partition string
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord) throws JsonProcessingException {
        String fieldValue = PartitionerUtil.getFieldValueAsString(sinkRecord, fieldName);

        return fieldName + "=" + fieldValue;
    }
}
