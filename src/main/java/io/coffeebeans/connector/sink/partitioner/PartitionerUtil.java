package io.coffeebeans.connector.sink.partitioner;

import io.coffeebeans.connector.sink.exception.PartitionException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PartitionerUtil {
    private static final Logger logger = LoggerFactory.getLogger(PartitionerUtil.class);

    public static String getFieldValueAsString(SinkRecord sinkRecord, String fieldName) {
        return getFieldValue(sinkRecord, fieldName).toString();
    }

    public static Object getFieldValue(SinkRecord sinkRecord, String fieldName) {
        if (sinkRecord.value() instanceof Map)
            return getFieldValueFromMap(sinkRecord, fieldName);
        else
            return getFieldValueFromStruct(sinkRecord, fieldName);
    }

    private static Object getFieldValueFromMap(SinkRecord record, String fieldName) {
        Map<?, ?> valueMap = (Map<?, ?>) record.value();
        return valueMap.get(fieldName);
    }

    private static Object getFieldValueFromStruct(SinkRecord sinkRecord, String fieldName) {
        Schema valueSchema = sinkRecord.valueSchema();
        Struct struct = (Struct) sinkRecord.value();

        Object fieldValue = struct.get(fieldName);
        Schema.Type type = valueSchema.field(fieldName).schema().type();

        switch (type) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case STRING:
            case BOOLEAN: return fieldValue;

            default: {
                logger.error("Type {} is not supported: ", type.getName());
                throw new PartitionException("Error retrieving field value");
            }
        }
    }
}
