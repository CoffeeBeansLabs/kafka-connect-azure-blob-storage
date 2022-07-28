package io.coffeebeans.connector.sink.format;

import java.io.IOException;

public interface SchemaStore {

    void register(String topic, String schemaFileURL) throws IOException;

    Object getSchema(String topic);
}
