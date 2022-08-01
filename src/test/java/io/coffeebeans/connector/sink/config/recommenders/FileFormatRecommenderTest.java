package io.coffeebeans.connector.sink.config.recommenders;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.format.FileFormat;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for FileFormatRecommender.
 */
public class FileFormatRecommenderTest {

    @Test
    @DisplayName("Should return valid file formats")
    public void validValues_givenConfig_shouldReturnSupportedFileFormats() {

        FileFormat[] supportedFileFormats = FileFormat.values();

        List<Object> expectedValues = Stream.of(supportedFileFormats)
                .map(FileFormat::name)
                .collect(Collectors.toList());

        List<Object> actualValues = new FileFormatRecommender().validValues(
                        AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY,
                        new HashMap<>()
                );

        Assertions.assertEquals(expectedValues, actualValues);
    }
}
