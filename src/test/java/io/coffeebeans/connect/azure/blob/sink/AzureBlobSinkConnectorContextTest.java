package io.coffeebeans.connect.azure.blob.sink;

import static org.mockito.Mockito.mock;

import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connect.azure.blob.sink.format.Format;
import io.coffeebeans.connect.azure.blob.sink.format.SchemaStore;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AzureBlobSinkConnectorContext}.
 */
public class AzureBlobSinkConnectorContextTest {

    private final Map<String, String> configProps = new HashMap<>();

    @Test
    @DisplayName("Given config with multiple topics, default Avro format, "
            + "but no schema url should not throw exception")
    void givenConfigProps_withMultipleTopics_withDefaultFileFormat_butNotSchemaUrl_shouldNotThrowException() {

        // Adding topics, schema url for each topic and file format configuration to the config props.
        String topicConfig = SinkTask.TOPICS_CONFIG;
        String topics = "alpha, beta, lambda";

        String fileFormatConfig = AzureBlobSinkConfig.FORMAT_CONF;
        String fileFormat = Format.AVRO.toString();

        configProps.put(topicConfig, topics);
        configProps.put(fileFormatConfig, fileFormat);

        SchemaStore mockedSchemaStore = mock(SchemaStore.class);

        Assertions.assertDoesNotThrow(() -> AzureBlobSinkConnectorContext.builder(configProps)
                        .withSchemaStore(mockedSchemaStore)
                        .build()
        );
    }
}
