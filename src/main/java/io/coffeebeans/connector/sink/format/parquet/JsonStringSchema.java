package io.coffeebeans.connector.sink.format.parquet;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;

public class JsonStringSchema {

    public static Schema avroSchema;

    public void readSchema(String filePath) throws IOException {
        avroSchema = new Schema.Parser().parse(new File(filePath));
    }

    public static Schema getSchema() {
        return avroSchema;
    }
}
