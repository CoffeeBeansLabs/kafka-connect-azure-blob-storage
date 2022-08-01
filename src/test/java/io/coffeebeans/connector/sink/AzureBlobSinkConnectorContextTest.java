package io.coffeebeans.connector.sink;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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

        String alphaTopicSchemaUrlConfig = "alpha.schema.url";
        String betaTopicSchemaUrlConfig = "beta.schema.url";
        String lambdaTopicSchemaUrlConfig = "lambda.schema.url";

        String alphaTopicSchemaUrlValue = "http://alpha.host.com/schema";
        String betaTopicSchemaUrlValue = "http://beta.host.com/schema";
        String lambdaTopicSchemaUrlValue = "http://lambda.host.com/schema";
        List<String> expectedSchemaUrls = new ArrayList<>() {
            {
                add(alphaTopicSchemaUrlValue);
                add(betaTopicSchemaUrlValue);
                add(lambdaTopicSchemaUrlValue);
            }
        };

        String fileFormatConfig = AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY;
        String fileFormat = FileFormat.PARQUET.toString();

        configProps.put(topicConfig, topics);
        configProps.put(alphaTopicSchemaUrlConfig, alphaTopicSchemaUrlValue);
        configProps.put(betaTopicSchemaUrlConfig, betaTopicSchemaUrlValue);
        configProps.put(lambdaTopicSchemaUrlConfig, lambdaTopicSchemaUrlValue);
        configProps.put(fileFormatConfig, fileFormat);

        // Mocking SchemaStore to check how many times the register method was called.
        SchemaStore mockedSchemaStore = mock(SchemaStore.class);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaUrlCaptor = ArgumentCaptor.forClass(String.class);


        AzureBlobSinkConnectorContext context = AzureBlobSinkConnectorContext.builder(configProps)
                .withSchemaStore(mockedSchemaStore)
                .build();

        verify(mockedSchemaStore, times(3))
                .register(
                        topicCaptor.capture(),
                        schemaUrlCaptor.capture()
                );

        List<String> capturedTopics = topicCaptor.getAllValues();
        List<String> capturedSchemaUrls = schemaUrlCaptor.getAllValues();

        Assertions.assertEquals(expectedTopics, capturedTopics);
        Assertions.assertEquals(expectedSchemaUrls, capturedSchemaUrls);
    }

    @Test
    @DisplayName("Given config with multiple topics, parquet as file format, "
            + "but no schema url should throw schema not found exception")
    void givenConfig_withMultipleTopics_withParquetFileFormat_butNotSchemaUrl_shouldThrowSchemaNotFoundException() {

        // Adding topics, schema url for each topic and file format configuration to the config props.
        String topicConfig = SinkTask.TOPICS_CONFIG;
        String topics = "alpha, beta, lambda";

        String fileFormatConfig = AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY;
        String fileFormat = FileFormat.PARQUET.toString();

        configProps.put(topicConfig, topics);
        configProps.put(fileFormatConfig, fileFormat);

        SchemaStore mockedSchemaStore = mock(SchemaStore.class);

        Assertions.assertThrowsExactly(SchemaNotFoundException.class,
                () -> AzureBlobSinkConnectorContext.builder(configProps)
                        .withSchemaStore(mockedSchemaStore)
                        .build(),
                "Schema url not configured for topic: alpha"
        );
    }

    @Test
    @DisplayName("Given config with multiple topics, default file format, "
            + "but no schema url should not throw exception")
    void givenConfigProps_withMultipleTopics_withDefaultFileFormat_butNotSchemaUrl_shouldNotThrowException() {

        // Adding topics, schema url for each topic and file format configuration to the config props.
        String topicConfig = SinkTask.TOPICS_CONFIG;
        String topics = "alpha, beta, lambda";

        String fileFormatConfig = AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY;
        String fileFormat = FileFormat.NONE.toString();

        configProps.put(topicConfig, topics);
        configProps.put(fileFormatConfig, fileFormat);

        SchemaStore mockedSchemaStore = mock(SchemaStore.class);

        Assertions.assertDoesNotThrow(() -> AzureBlobSinkConnectorContext.builder(configProps)
                        .withSchemaStore(mockedSchemaStore)
                        .build()
        );
    }
}
