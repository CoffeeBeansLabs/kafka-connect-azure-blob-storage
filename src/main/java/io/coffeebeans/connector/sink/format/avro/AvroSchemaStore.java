package io.coffeebeans.connector.sink.format.avro;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To write JSON string values by any writer (like ParquetWriter),
 * it needs Schema of the data during initialization.
 * Extracting schema from JSON String will have inconsistencies
 * and will miss out information like default value, type
 * unions etc. Instead of that it is better to load Avro schema
 * for that data from a schema file.
 *
 * <p>The IO operation to load schema from a file and parsing it
 * is an expensive operation therefore, it is better to do it
 * once and store it in a static variable so that there is no
 * need of loading and parsing of schema everytime a
 * new RecordWriter is initialized.
 */
public class AvroSchemaStore {
    private static final Logger log = LoggerFactory.getLogger(AvroSchemaStore.class);

    private static Schema schema;

    /**
     * It will load the schema from the file, parse it using Avro schema parser and store it in a static variable.
     *
     * @param file Absolute file path
     * @throws IOException Thrown when unable to find the file, open the file or read contents from it
     */
    public static void loadFromFile(String file) throws IOException {
        log.info("Loading avro schema from file: {}", file);
        try {
            schema = new Schema.Parser().parse(new File(file));
        } catch (IOException e) {
            log.error("Failed to load schema from file with exception: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Get the Avro schema parsed from the schema file.
     *
     * @return Avro schema
     */
    public static Schema get() {
        return schema;
    }
}
