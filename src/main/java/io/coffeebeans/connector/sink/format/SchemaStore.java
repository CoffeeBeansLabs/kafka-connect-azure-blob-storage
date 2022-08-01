package io.coffeebeans.connector.sink.format;

import java.io.IOException;

/**
 * Schema store will load, parse and store the topics and their respective
 * schemas.
 */
public interface SchemaStore {

    void register(String topic, String schemaFileUrl) throws IOException;

    Object getSchema(String topic);

    void clear();
}
