package io.coffeebeans.connector.sink.format.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * To build avro schema.
 */
@Deprecated
public class AvroSchemaBuilder {
    private static final Logger logger = LoggerFactory.getLogger(AvroSchemaBuilder.class);
    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String INT = "int";
    private static final String LONG = "long";
    private static final String FLOAT = "float";
    private static final String DOUBLE = "double";
    private static final String STRING = "string";
    private static final String BOOLEAN = "boolean";
    private static final String NULL = "null";
    private static final String ARRAY = "array";
    private static final String ITEMS = "items";
    private static final String RECORD = "record";
    private static final String FIELDS = "fields";


    private final ObjectMapper objectMapper;

    public AvroSchemaBuilder(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * I build Avro schema from JsonNode.
     *
     * @param jsonNode JsonNode from which avro schema will be generated
     * @return Schema - Avro schema
     * @throws JsonProcessingException Throws exception if error occur during processing JsonNode
     */
    public Schema buildAvroSchema(JsonNode jsonNode) throws JsonProcessingException {

        return new Schema.Parser().parse(buildAvroSchemaAsString(jsonNode));
    }

    /**
     * I build Avro schema string from JsonNode.
     *
     * @param jsonNode JsonNode
     * @return String - Avro schema string
     * @throws JsonProcessingException Throws if error occur during writing object node as string
     */
    public String buildAvroSchemaAsString(JsonNode jsonNode) throws JsonProcessingException {
        ObjectNode objectNode = objectMapper.createObjectNode();

        objectNode.put("namespace", "blob.sink");
        objectNode.put(NAME, "Record");
        objectNode.put(TYPE, RECORD);
        objectNode.set(FIELDS, getFields(jsonNode));

        return objectMapper.writeValueAsString(objectNode);
    }

    /**
     * I process the JsonNode and return all ArrayNode of fields in the json node.
     *
     * @param jsonNode JsonNode
     * @return ArrayNode of fields in json node
     */
    private ArrayNode getFields(JsonNode jsonNode) {
        // ArrayNode to store field types.
        final ArrayNode fields = objectMapper.createArrayNode();

        final Iterator<Map.Entry<String, JsonNode>> jsonNodeFieldsIterator = jsonNode.fields();
        Map.Entry<String, JsonNode> jsonNodeField;

        while (jsonNodeFieldsIterator.hasNext()) {
            jsonNodeField = jsonNodeFieldsIterator.next();
            String fieldType = getFieldType(jsonNodeField.getValue());

            if (ARRAY.equals(fieldType)) {
                ArrayNode arrayNode = (ArrayNode) jsonNodeField;
                JsonNode element = arrayNode.get(0);
                ObjectNode objectNode = objectMapper.createObjectNode();
                objectNode.put(NAME, jsonNodeField.getKey());

                if (arrayNode.size() == 0 || element == null) {
                    throw new RuntimeException("Unable to determine field type from empty list");
                }

                String elementFieldType = getFieldType(element);
                if (RECORD.equals(elementFieldType)) {
                    objectNode.set(TYPE, objectMapper.createObjectNode().put(TYPE, ARRAY)
                            .set(ITEMS, objectMapper.createObjectNode().put(TYPE, RECORD)
                                    .put(NAME, getKeyWithRandomNumber(jsonNodeField.getKey()))
                                    .set(FIELDS, getFields(element))));
                } else {
                    objectNode.set(TYPE, objectMapper.createObjectNode().put(TYPE, ARRAY).put(ITEMS, elementFieldType));
                }
                fields.add(objectNode);

            } else if (RECORD.equals(fieldType)) {
                ObjectNode objectNode = objectMapper.createObjectNode();
                objectNode.put(NAME, jsonNodeField.getKey());
                objectNode.set(TYPE, objectMapper.createObjectNode().put(TYPE, RECORD)
                        .put(NAME, getKeyWithRandomNumber(jsonNodeField.getKey()))
                        .set(FIELDS, getFields(jsonNodeField.getValue())));
                fields.add(objectNode);

            } else if (NULL.equals(fieldType)) {
                ObjectNode objectNode = objectMapper.createObjectNode();
                objectNode.put(NAME, jsonNodeField.getKey());
                objectNode.putArray(TYPE).add(NULL);
                fields.add(objectNode);

            } else {
                fields.add(objectMapper.createObjectNode().put(NAME, jsonNodeField.getKey()).put(TYPE, fieldType));
            }
        }

        return fields;
    }

    /**
     * I process the JsonNode and return the field type as String.
     *
     * @param jsonNode JsonNode
     * @return String - Field type of the json node
     */
    private String getFieldType(JsonNode jsonNode) {
        switch (jsonNode.getNodeType()) {

            case STRING: return STRING;

            case NUMBER: {
                if (jsonNode.isInt()) {
                    return INT;
                } else if (jsonNode.isLong()) {
                    return LONG;
                } else if (jsonNode.isFloat()) {
                    return FLOAT;
                } else {
                    return DOUBLE;
                }
            }

            case BOOLEAN: return BOOLEAN;

            case ARRAY: return ARRAY;

            case OBJECT: return RECORD;

            case NULL: return NULL;

            default: {
                logger.error("Node type {} is not supported", jsonNode.getNodeType());
                throw new RuntimeException("Node type not supported: " + jsonNode.getNodeType());
            }
        }
    }

    /**
     * I take a key and append it with a random number.
     *
     * @param key String key
     * @return String - key appended with random number
     */
    private String getKeyWithRandomNumber(String key) {
        return key + "_" + new Random().nextInt(100);
    }
}
