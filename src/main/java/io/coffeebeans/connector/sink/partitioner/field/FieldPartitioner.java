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
    public static final Logger logger = LoggerFactory.getLogger(FieldPartitioner.class);

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
        String topic = sinkRecord.topic();
        long partition = sinkRecord.kafkaPartition();
        String fieldValue = PartitionerUtil.getFieldValueAsString(sinkRecord, fieldName);

        /*
          Output format:
          <prefix>/<kafkaTopic>/<fieldName>=<fieldValue>/<kafkaTopic>+<kafkaPartition>+<startOffset>.<format>
         */

        return prefix + FOLDER_DELIMITER // <prefix>/

                // <kafkaTopic>/
                + topic + FOLDER_DELIMITER

                // <fieldName>=<fieldValue>/
                + fieldName + "=" + fieldValue + FOLDER_DELIMITER

                // <kafkaTopic>+<kafkaPartition>+<startingOffset>
                + topic + FILE_DELIMITER + partition + FOLDER_DELIMITER + startingOffset;
    }

    //    private String getFieldValue(SinkRecord sinkRecord, String fieldName) {
    //        if (sinkRecord.value() instanceof Map)
    //            return getFieldValueFromMap(sinkRecord, fieldName);
    //        else
    //            return getFieldValueFromStruct(sinkRecord, fieldName);
    //    }
    //
    //    private String getFieldValueFromMap(SinkRecord record, String fieldName) {
    //        Map<?, ?> valueMap = (Map<?, ?>) record.value();
    //        return valueMap.get(fieldName).toString();
    //    }
    //
    //    private String getFieldValueFromStruct(SinkRecord sinkRecord, String fieldName) {
    //        Schema valueSchema = sinkRecord.valueSchema();
    //        Struct struct = (Struct) sinkRecord.value();
    //
    //        Object fieldValue = struct.get(fieldName);
    //        Schema.Type type = valueSchema.field(fieldName).schema().type();
    //
    //        switch (type) {
    //            case INT8:
    //            case INT16:
    //            case INT32:
    //            case INT64: {
    //                Number record = (Number) fieldValue;
    //                return record.toString();
    //            }
    //
    //            case STRING: return (String) fieldValue;
    //            case BOOLEAN: {
    //                boolean booleanValue = (boolean) fieldValue;
    //                return Boolean.toString(booleanValue);
    //            }
    //
    //            default: {
    //                logger.error("Type {} is not supported as a partition key", type.getName());
    //                throw new PartitionException("Error encoding partition");
    //            }
    //        }
    //    }
}
