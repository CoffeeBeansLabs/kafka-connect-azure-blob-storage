package io.coffeebeans.connector.sink;

import static org.mockito.Mockito.times;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.exception.SchemaNotFoundException;
import io.coffeebeans.connector.sink.format.FileFormat;
import io.coffeebeans.connector.sink.format.SchemaStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for AzureBlobSinkConnectorContext.
 */
public class AzureBlobSinkConnectorContextTest {

    private final Map<String, String> configProps = new HashMap<>();

    @Test
    @DisplayName("Given config with multiple topics, parquet file format it should call "
            + "SchemaStore register method for each topic")
    void givenConfigProps_withMultipleTopics_withParquetFileFormat_shouldCallSchemaStoreRegisterForEachTopic()
            throws IOException, SchemaNotFoundException {

        // Adding topics, schema url for each topic and file format configuration to the config props.
        String topicConfig = SinkTask.TOPICS_CONFIG;
        String topics = "alpha, beta, lambda";
        List<String> expectedTopics = new ArrayList<>() {
            {
                add("alpha");
                add("beta");
                add("lambda");
            }
        };

        String alphaTopicSchemaURLConfig = "alpha.schema.url";
        String betaTopicSchemaURLConfig = "beta.schema.url";
        String lambdaTopicSchemaURLConfig = "lambda.schema.url";

        String alphaTopicSchemaURLValue = "http://alpha.host.com/schema";
        String betaTopicSchemaURLValue = "http://beta.host.com/schema";
        String lambdaTopicSchemaURLValue = "http://lambda.host.com/schema";
        List<String> expectedSchemaURLs = new ArrayList<>() {
            {
                add(alphaTopicSchemaURLValue);
                add(betaTopicSchemaURLValue);
                add(lambdaTopicSchemaURLValue);
            }
        };

        String fileFormatConfig = AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY;
        String fileFormat = FileFormat.PARQUET.toString();

        configProps.put(topicConfig, topics);
        configProps.put(alphaTopicSchemaURLConfig, alphaTopicSchemaURLValue);
        configProps.put(betaTopicSchemaURLConfig, betaTopicSchemaURLValue);
        configProps.put(lambdaTopicSchemaURLConfig, lambdaTopicSchemaURLValue);
        configProps.put(fileFormatConfig, fileFormat);

        // Mocking SchemaStore to check how many times the register method was called.
        SchemaStore mockedSchemaStore = Mockito.mock(SchemaStore.class);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaURLCaptor = ArgumentCaptor.forClass(String.class);


        AzureBlobSinkConnectorContext context = AzureBlobSinkConnectorContext.builder(configProps)
                .schemaStore(mockedSchemaStore)
                .build();

        Mockito.verify(mockedSchemaStore, times(3))
                .register(
                        topicCaptor.capture(),
                        schemaURLCaptor.capture()
                );

        List<String> capturedTopics = topicCaptor.getAllValues();
        List<String> capturedSchemaURLs = schemaURLCaptor.getAllValues();

        Assertions.assertEquals(expectedTopics, capturedTopics);
        Assertions.assertEquals(expectedSchemaURLs, capturedSchemaURLs);
    }

    @Test
    @DisplayName("Given config with multiple topics, parquet as file format, "
            + "but no schema url should throw schema not found exception")
    void givenConfigProps_withMultipleTopics_withParquetFileFormat_butNotSchemaURL_shouldThrowSchemaNotFoundException() {

        // Adding topics, schema url for each topic and file format configuration to the config props.
        String topicConfig = SinkTask.TOPICS_CONFIG;
        String topics = "alpha, beta, lambda";

        String fileFormatConfig = AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY;
        String fileFormat = FileFormat.PARQUET.toString();

        configProps.put(topicConfig, topics);
        configProps.put(fileFormatConfig, fileFormat);

        SchemaStore mockedSchemaStore = Mockito.mock(SchemaStore.class);

        Assertions.assertThrowsExactly(SchemaNotFoundException.class,
                () -> AzureBlobSinkConnectorContext.builder(configProps)
                        .schemaStore(mockedSchemaStore)
                        .build(),
                "Schema url not configured for topic: alpha"
        );
    }

    @Test
    @DisplayName("Given config with multiple topics, default file format, "
            + "but no schema url should not throw exception")
    void givenConfigProps_withMultipleTopics_withDefaultFileFormat_butNotSchemaURL_shouldNotThrowException() {

        // Adding topics, schema url for each topic and file format configuration to the config props.
        String topicConfig = SinkTask.TOPICS_CONFIG;
        String topics = "alpha, beta, lambda";

        String fileFormatConfig = AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY;
        String fileFormat = FileFormat.NONE.toString();

        configProps.put(topicConfig, topics);
        configProps.put(fileFormatConfig, fileFormat);

        SchemaStore mockedSchemaStore = Mockito.mock(SchemaStore.class);

        Assertions.assertDoesNotThrow(() -> AzureBlobSinkConnectorContext.builder(configProps)
                        .schemaStore(mockedSchemaStore)
                        .build()
        );
    }
}
