package io.coffeebeans.connect.azure.blob.sink.partitioner.field;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connect.azure.blob.sink.exception.PartitionException;
import io.coffeebeans.connect.azure.blob.sink.partitioner.DefaultPartitioner;
import io.coffeebeans.connect.azure.blob.sink.partitioner.PartitionerUtil;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * This partitioner will partition the incoming records based on the value of the field specified.
 */
public class FieldPartitioner extends DefaultPartitioner {
    private final String fieldName;

    /**
     * Constructs {@link FieldPartitioner}.
     *
     * @param config Connector configuration
     */
    public FieldPartitioner(AzureBlobSinkConfig config) {
        super(config);

        fieldName = config.getFieldName();
        log.debug("Field name configured: {}", fieldName);
    }

    /**
     * Generate the encoded partition string<br>
     * by extracting the value of the<br>
     * specified field.<br>
     * <pre>
     *     <code>
     *
     *          &lt;fieldName&gt;=&lt;fieldValue&gt;
     *     </code>
     * </pre>
     *
     * @param sinkRecord The sink record to be stored
     * @return Encoded partition string
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord) throws PartitionException {

        String fieldValue = null;
        try {
            fieldValue = PartitionerUtil.getFieldValueAsString(sinkRecord, fieldName);

        } catch (JsonProcessingException e) {
            throw new PartitionException(e);
        }
        return fieldName + "=" + fieldValue;
    }
}
