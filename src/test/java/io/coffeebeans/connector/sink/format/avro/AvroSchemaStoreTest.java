package io.coffeebeans.connector.sink.format.avro;

import io.coffeebeans.connector.sink.HttpUrlStreamHandler;
import io.coffeebeans.connector.sink.format.SchemaStore;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandlerFactory;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * AvroSchemaStore unit test class.
 */
public class AvroSchemaStoreTest {

    private static HttpUrlStreamHandler httpUrlStreamHandler;
    private static SchemaStore schemaStore;

    private static final String KAFKA_TOPIC = "test";


    /**
     * Mocking URLStreamHandlerFactory to return custom HTTP url stream handler.
     * Custom HTTP URL Stream handler never calls the passed URL.
     */
    @BeforeAll
    public static void setupUrlStreamHandlerFactory() {
        URLStreamHandlerFactory urlStreamHandlerFactory = Mockito.mock(URLStreamHandlerFactory.class);
        URL.setURLStreamHandlerFactory(urlStreamHandlerFactory);

        httpUrlStreamHandler = new HttpUrlStreamHandler();
        Mockito.when(
                urlStreamHandlerFactory.createURLStreamHandler("http")
        ).thenReturn(httpUrlStreamHandler);

        // Assigning schema store
        schemaStore = AvroSchemaStore.getSchemaStore();
    }

    @BeforeEach
    public void reset() {
        httpUrlStreamHandler.resetConnections();
        schemaStore.clear();
    }

    /**
     * Given the topic-partition and schema file url, the register method
     * should download the file, parse and store it in the schema map.
     * <br>
     * In this test the URLConnection is mocked so that it returns the
     * expected schema input stream.
     * <br>
     * Schema Parser is not mocked for purpose.
     *
     * @throws IOException thrown when encounters an exception during URL call or parsing the schema.
     */
    @Test
    @DisplayName("register method should load and save schema when given correct url")
    void register_givenCorrectSchemaUrl_shouldLoadSchema() throws IOException {

        String givenSchemaUrl = "http://host/schema";

        String schemaFile = "avro-schema.avsc";
        InputStream expectedSchemaInputStream = getClass()
                .getClassLoader()
                .getResourceAsStream(schemaFile);

        assert expectedSchemaInputStream != null;
        String expectedSchemaString = new String(expectedSchemaInputStream.readAllBytes());

        // Mock URLConnection
        URLConnection urlConnection = Mockito.mock(URLConnection.class);
        httpUrlStreamHandler.addConnection(new URL(givenSchemaUrl), urlConnection);

        Mockito
            .when(urlConnection.getInputStream())
            .thenReturn(
                    new ByteArrayInputStream(expectedSchemaString.getBytes())
            );

        // Action
        schemaStore.register(KAFKA_TOPIC, givenSchemaUrl);

        // Assertion
        Schema expectedSchema = new Schema.Parser().parse(expectedSchemaString);
        Schema actualSchema = (Schema) schemaStore.getSchema(KAFKA_TOPIC);

        Assertions.assertEquals(expectedSchema, actualSchema);
    }

    /**
     * Given Malformed URL, the register method should throw MalformedURLException.
     */
    @Test
    @DisplayName("register method should throw MalformedURLException if given malformed url")
    void register_givenMalformedSchemaUrl_shouldThrowMalformedUrlException() {

        String givenSchemaUrl = "htp//host/schema";

        Assertions.assertThrows(MalformedURLException.class,
                () -> schemaStore.register(KAFKA_TOPIC, givenSchemaUrl)
        );
    }

    /**
     * Given correct schema url, the register method should throw an IOException if it
     * encounters any issue while downloading the data from the URL.
     *
     * @throws IOException Thrown if URLConnection encounters any issue while opening the stream
     */
    @Test
    @DisplayName("register method should throw IOException if encounters any problem when downloading the schema")
    void register_givenCorrectSchemaUrl_shouldThrowException_whenEncountersAnyException_whileDownloadingSchema()
            throws IOException {

        String givenSchemaUrl = "http://host/schema";

        // Mock URLConnection
        URLConnection urlConnection = Mockito.mock(URLConnection.class);
        httpUrlStreamHandler.addConnection(
                new URL(givenSchemaUrl),
                urlConnection
        );

        Mockito
                .when(urlConnection.getInputStream())
                .thenThrow(IOException.class);

        // Assertion
        Assertions.assertThrows(IOException.class,
                () -> schemaStore.register(KAFKA_TOPIC, givenSchemaUrl)
        );
    }

}
