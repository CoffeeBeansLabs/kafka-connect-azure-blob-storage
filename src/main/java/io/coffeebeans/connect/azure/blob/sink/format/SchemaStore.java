package io.coffeebeans.connect.azure.blob.sink.format;

import io.coffeebeans.connect.azure.blob.sink.exception.SchemaParseException;

/**
 * Schema store will load, parse and store the topics and their respective
 * schemas.
 */
public interface SchemaStore {

    void register(String topic, String schemaFileUrl) throws SchemaParseException;

    Object getSchema(String topic);

    void clear();
}
