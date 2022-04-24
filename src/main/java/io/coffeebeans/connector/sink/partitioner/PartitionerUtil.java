package io.coffeebeans.connector.sink.partitioner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.exception.PartitionException;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to extract value from the field of the record.
 */
public class PartitionerUtil {
    private static final Logger logger = LoggerFactory.getLogger(PartitionerUtil.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String getFieldValueAsString(SinkRecord sinkRecord, String fieldName) throws JsonProcessingException {
        return getFieldValue(sinkRecord, fieldName).toString();
    }

    /**
     * Return the value of the field specified.
     *
     * @param sinkRecord SinkRecord
     * @param fieldName Name of the field
     * @return Field value
     */
    public static Object getFieldValue(SinkRecord sinkRecord, String fieldName) throws JsonProcessingException {
        if (sinkRecord.value() instanceof Map) {
            return getFieldValueFromMap(sinkRecord, fieldName);
        } else if (sinkRecord.value() instanceof String) {
            return getFieldValueFromJsonString(sinkRecord, fieldName);
        }
        return getFieldValueFromStruct(sinkRecord, fieldName);
    }

    private static Object getFieldValueFromJsonString(SinkRecord record, String fieldName) throws
            JsonProcessingException {

        Map<?, ?> valueMap;
        try {
            valueMap = objectMapper.readValue((String) record.value(), Map.class);
            return valueMap.get(fieldName);

        } catch (JsonProcessingException e) {
            logger.error("Error getting value from field name: {}, with exception {}", fieldName, e.getMessage());
            throw e;
        }
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
          case BOOLEAN: {
              return fieldValue;
          }

          default: {
              logger.error("Type {} is not supported: ", type.getName());
              throw new PartitionException("Error retrieving field value");
          }
        }
    }
}
