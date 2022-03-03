package io.coffeebeans.connector.sink.partitioner;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.exception.PartitionException;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class FieldPartitioner implements Partitioner {
    public static final Logger logger = LoggerFactory.getLogger(FieldPartitioner.class);
    private String prefix;
    private String partitionKey;

    @Override
    public void configure(Map<String, String> configProps) {
        this.prefix = configProps.get(AzureBlobSinkConfig.TOPIC_DIR);
        this.partitionKey = configProps.get(AzureBlobSinkConfig.PARTITION_STRATEGY_FIELD_CONF);
    }

    /**
     * Return file path based on the field name
     * @param sinkRecord The record to be stored
     * @param startingOffset kafka offset of the first record of the batch
     * @return <prefix>/<topic>/<fieldName>=<fieldValue>/<topic>+<kafkaPartition>+<startOffset>.<format>
     */
    @Override
    public String encodePartition(SinkRecord sinkRecord, long startingOffset) {
        String topic = sinkRecord.topic();
        long partition = sinkRecord.kafkaPartition();
        String partitionValue = getFieldValue(sinkRecord, partitionKey);

        return prefix + folderDelimiter + // <prefix>/
                topic + folderDelimiter + // <topic>/
                partitionKey + "=" + partitionValue + folderDelimiter + // <fieldName>=<fieldValue>/
                topic + fileDelimiter + partition + fileDelimiter + startingOffset + // <topic>+<kafkaPartition>+<startOffset>
                "." + format; // .<format>
    }

    private String getFieldValue(SinkRecord sinkRecord, String fieldName) {
        if (sinkRecord.value() instanceof Map)
            return getFieldValueFromMap(sinkRecord, fieldName);
        else
            return getFieldValueFromStruct(sinkRecord, fieldName);
    }

    private String getFieldValueFromMap(SinkRecord record, String fieldName) {
        Map<?, ?> valueMap = (Map<?, ?>) record.value();
        return valueMap.get(fieldName).toString();
    }

    private String getFieldValueFromStruct(SinkRecord sinkRecord, String fieldName) {
        Schema valueSchema = sinkRecord.valueSchema();
        Struct struct = (Struct) sinkRecord.value();

        Object fieldValue = struct.get(fieldName);
        Schema.Type type = valueSchema.field(fieldName).schema().type();

        switch (type) {
            case INT8:
            case INT16:
            case INT32:
            case INT64: {
                Number record = (Number) fieldValue;
                return record.toString();
            }

            case STRING: return (String) fieldValue;
            case BOOLEAN: {
                boolean booleanValue = (boolean) fieldValue;
                return Boolean.toString(booleanValue);
            }

            default: {
                logger.error("Type {} is not supported as a partition key", type.getName());
                throw new PartitionException("Error encoding partition");
            }
        }
    }
}
