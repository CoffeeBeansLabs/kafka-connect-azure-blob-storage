package io.coffeebeans.connector.sink.util;

import static org.apache.kafka.connect.data.Schema.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

/**
 * Utility class to convert the struct data structure to Map.
 */
public class StructToMap {

    /**
     * I need the Struct as parameter. I will convert it into the Map and return it.
     *
     * @param struct Struct to be converted to map
     * @return Map
     */
    public static Map<String, Object> toMap(Struct struct) {
        Map<String, Object> valueMap = new HashMap<>();

        // Get list of fields in the struct
        List<Field> fields = struct.schema().fields();

        for (Field field : fields) {
            insertFieldToMap(struct, valueMap, field);
        }

        return valueMap;
    }

    private static void insertFieldToMap(Struct struct, Map<String, Object> valueMap, Field field) {
        String fieldName = field.name();
        Type fieldType = field.schema().type();
        String schemaName = field.schema().name();

        switch (fieldType) {
            case STRING: valueMap.put(fieldName, struct.getString(fieldName));
            break;

            case INT16: valueMap.put(fieldName, struct.getInt16(fieldName));
            break;

            case INT32: {
                if (Date.LOGICAL_NAME.equals(fieldName) || Time.LOGICAL_NAME.equals(fieldName)) {
                    valueMap.put(fieldName, struct.get(fieldName));
                } else {
                    valueMap.put(fieldName, struct.getInt32(fieldName));
                }
            }
            break;

            case INT64: {
                if (Timestamp.LOGICAL_NAME.equals(fieldName)) {
                    valueMap.put(fieldName, struct.get(fieldName));
                } else {
                    valueMap.put(fieldName, struct.getInt64(fieldName));
                }
            }
            break;

            case FLOAT32: valueMap.put(fieldName, struct.getFloat32(fieldName));
            break;

            case FLOAT64: valueMap.put(fieldName, struct.getFloat64(fieldName));
            break;

            case BOOLEAN: valueMap.put(fieldName, struct.getBoolean(fieldName));
            break;

            case ARRAY: {
                List<Object> fieldArray = struct.getArray(fieldName);

                if (fieldArray.get(0) instanceof Struct) {
                    List<Object> jsonArray = new ArrayList<>();

                    fieldArray.forEach(item -> jsonArray.add(toMap(struct.getStruct(fieldName))));
                    valueMap.put(fieldName, jsonArray);

                } else {
                    valueMap.put(fieldName, fieldArray);
                }
            }
            break;

            case STRUCT: valueMap.put(fieldName, toMap(struct));
            break;

            default: valueMap.put(fieldName, struct.get(fieldName));
        }
    }
}
