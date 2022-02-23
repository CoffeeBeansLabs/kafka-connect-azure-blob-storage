package io.coffeebeans.connector.sink.util;

import org.apache.kafka.connect.data.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StructToMap {

    public static Map<String, Object> toJsonMap(Struct struct) {
        Map<String, Object> jsonMap = new HashMap<>();

        // Get list of fields in the struct
        List<Field> fields = struct.schema().fields();

        for (Field field : fields) {
            insertFieldToMap(struct, jsonMap, field);
        }

        return jsonMap;
    }

    private static void insertFieldToMap(Struct struct, Map<String, Object> jsonMap, Field field) {
        String fieldName = field.name();
        Schema.Type fieldType = field.schema().type();
        String schemaName = field.schema().name();

        switch (fieldType) {
            case STRING: jsonMap.put(fieldName, struct.getString(fieldName));
                break;

            case INT16: jsonMap.put(fieldName, struct.getInt16(fieldName));
            break;

            case INT32: {
                if (Date.LOGICAL_NAME.equals(fieldName) || Time.LOGICAL_NAME.equals(fieldName)) {
                    jsonMap.put(fieldName, (java.util.Date) struct.get(fieldName));
                } else {
                    jsonMap.put(fieldName, struct.getInt32(fieldName));
                }
            }
            break;

            case INT64: {
                if (Timestamp.LOGICAL_NAME.equals(fieldName)) {
                    jsonMap.put(fieldName, (java.util.Date) struct.get(fieldName));
                } else {
                    jsonMap.put(fieldName, struct.getInt64(fieldName));
                }
            }
            break;

            case FLOAT32: jsonMap.put(fieldName, struct.getFloat32(fieldName));
            break;

            case FLOAT64: jsonMap.put(fieldName, struct.getFloat64(fieldName));
            break;

            case BOOLEAN: jsonMap.put(fieldName, struct.getBoolean(fieldName));
            break;

            case ARRAY: {
                List<Object> fieldArray = struct.getArray(fieldName);

                if (fieldArray.get(0) instanceof Struct) {
                    List<Object> jsonArray = new ArrayList<>();

                    fieldArray.forEach(item -> {
                        jsonArray.add(toJsonMap((Struct) struct.getStruct(fieldName)));
                    });
                    jsonMap.put(fieldName, jsonArray);
                } else {
                    jsonMap.put(fieldName, fieldArray);
                }
            }
            break;

            case STRUCT: jsonMap.put(fieldName, toJsonMap(struct));
            break;

            default: jsonMap.put(fieldName, struct.get(fieldName));
        }
    }
}
