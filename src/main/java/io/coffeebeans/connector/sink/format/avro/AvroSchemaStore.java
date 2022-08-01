package io.coffeebeans.connector.sink.format.avro;

import io.coffeebeans.connector.sink.format.SchemaStore;
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
 * It's a singleton class to store the topic and its respective Avro schema.
 * It can download the schema data from URL and store it after parsing it.
 * <br>
 * It's possible to have separate schema for each partition but that is
 * not supported currently.
 */
public class AvroSchemaStore implements SchemaStore {
    private static final Logger log = LoggerFactory.getLogger(AvroSchemaStore.class);

    private final Schema.Parser schemaParser;
    private final Map<String, Schema> schemaMap;

    /**
     * Singleton.
     */
    public AvroSchemaStore() {
        schemaParser = new Schema.Parser();
        schemaMap = new HashMap<>();
    }

    /**
     * Initializes (if not did already) the AvroSchemaStore instance and return it.
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
     * @throws IOException Thrown if the provided URL is malformed or if it encounters any issue
     *      while downloading the schema.
     */
    public void register(String topic, String schemaFileUrl) throws IOException {
        if (schemaMap.containsKey(topic)) {
            return;
        }
        try {
            Schema schema = loadFromUrl(schemaFileUrl);
            schemaMap.put(topic, schema);

        } catch (IOException e) {
            log.info("Failed to register schema for topic: {}", topic);
            throw e;
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
        log.info("Loading avro schema from url: {}", url);
        try {
            URL schemaUrl = new URL(url);
            return loadFromUrl(schemaUrl);

        } catch (MalformedURLException e) {
            log.error("Malformed URL: {}", url);
            throw e;

        } catch (IOException e) {
            log.error("Error reading data from URL: {}", url);
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
        log.info("Downloading avro schema from URL: {}", url);
        try (InputStream inputStream = getInputStream(url)) {
            return schemaParser.parse(inputStream);

        } catch (IOException e) {
            log.error("Failed to download schema from URL: {}", url);
            throw e;
        }
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
        try {
            // BufferedInputStream is much better in terms of performance.
            return new BufferedInputStream(url.openStream());

        } catch (IOException e) {
            log.error("Error opening stream on the provided url: {} ", url);
            log.error("Failed with exception: {}", e.getMessage());

            throw e;
        }
    }

    /**
     * Get the schema for the given topic.
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
