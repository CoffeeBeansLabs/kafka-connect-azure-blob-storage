package io.coffeebeans.connect.azure.blob.sink.format.avro;

import io.coffeebeans.connect.azure.blob.sink.exception.SchemaParseException;
import io.coffeebeans.connect.azure.blob.sink.format.SchemaStore;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores the topic and its respective Avro schema.<br>
 * It can download the schema data from URL and store it after parsing it.
 */
public class AvroSchemaStore implements SchemaStore {
    private static final Logger log = LoggerFactory.getLogger(AvroSchemaStore.class);

    private final Schema.Parser schemaParser;
    private final Map<String, Schema> schemaMap;

    /**
     * Constructs a {@link AvroSchemaStore}.
     */
    public AvroSchemaStore() {
        schemaParser = new Schema.Parser();
        schemaMap = new HashMap<>();
    }

    /**
     * Initializes the AvroSchemaStore instance and return it.
     *
     * @return Instance of AvroSchemaStore
     */
    public static AvroSchemaStore getSchemaStore() {
        return new AvroSchemaStore();
    }

    /**
     * Each Topic can have its own schema, so it is important to store
     * schema for each one of them.
     * <br>
     * Downloads the schema from the given URL, parse it and store it with the
     * given topic.
     * <br>
     * Topic is treated as the key and the Schema as the value.
     * <br>
     * Possible values for Schema URL->
     * <blockquote>
     *     <pre>
     *         From remote server: http://localhost:8080/schema
     *         From local file system: file:///path/to/schema/file
     *     </pre>
     * </blockquote>
     *
     * @param topic Topic as key for storing the schema w.r.t it
     * @param schemaFileUrl The URL from where the schema has to be downloaded
     * @throws SchemaParseException Thrown if encounters any error while downloading and parsing
     *      the schema file.
     */
    public void register(String topic, String schemaFileUrl) {
        if (schemaMap.containsKey(topic)) {
            return;
        }
        try {
            Schema schema = loadFromUrl(schemaFileUrl);
            schemaMap.put(topic, schema);

        } catch (IOException e) {
            log.error("Failed to register schema for topic: {} from {}", topic, schemaFileUrl);
            throw new SchemaParseException("Error registering schema", e);
        }
    }

    /**
     * Fetch the schema from the given url and parse it as an Avro schema.
     * <br>
     * Possible values ->
     * <blockquote>
     *     <pre>
     *         From remote server: http://localhost:8080/schema
     *         From local file system: file:///path/to/schema/file
     *     </pre>
     * </blockquote>
     *
     * @param url URL from which data has to be fetched
     * @return Parsed Avro schema
     * @throws IOException Is thrown if it encounters any issue while fetching the data from the given url
     */
    public Schema loadFromUrl(String url) throws IOException {
        try {
            URL schemaUrl = new URL(url);
            return loadFromUrl(schemaUrl);

        } catch (MalformedURLException e) {
            log.error("Malformed URL: {}", url);
            throw e;
        }
    }

    /**
     * Fetch the schema from the given url and parse it as an Avro schema.
     *
     * @param url URL from which data has to be fetched
     * @return Parsed Avro schema
     * @throws IOException Is thrown if it encounters any issue while fetching the data from the given url
     */
    public Schema loadFromUrl(URL url) throws IOException {

        InputStream inputStream = getInputStream(url);
        return schemaParser.parse(inputStream);
    }

    /**
     * Opens the input stream on the provided URL and returns an InputStream.
     * BufferedInputStream improves the performance.
     *
     * @param url URL of the resource
     * @return InputStream
     * @throws IOException thrown if encounters an exception while opening the input stream
     */
    private InputStream getInputStream(URL url) throws IOException {

        // BufferedInputStream is much better in terms of performance.
        return new BufferedInputStream(url.openStream());
    }

    /**
     * Get schema for the topic.
     *
     * @param topic kafka topic
     * @return Stored Avro schema
     */
    public Schema getSchema(String topic) {
        return this.schemaMap
                .get(topic);
    }

    /**
     * Clears the map containing topic and respective schemas.
     */
    @Override
    public void clear() {
        schemaMap.clear();
    }
}
