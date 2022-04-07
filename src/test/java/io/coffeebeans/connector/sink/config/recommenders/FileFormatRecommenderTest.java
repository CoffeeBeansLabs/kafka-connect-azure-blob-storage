package io.coffeebeans.connector.sink.config.recommenders;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.config.recommenders.partitioner.StrategyRecommender;
import io.coffeebeans.connector.sink.format.FileFormat;
import io.coffeebeans.connector.sink.partitioner.PartitionStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class FileFormatRecommenderTest {

    @Test
    @DisplayName("Should return valid file formats")
    public void shouldReturnValidFileFormats() {
        List<Object> expectedValues = new ArrayList<>();
        Arrays.stream(FileFormat.values()).forEach((format) -> expectedValues.add(format.toString()));

        List<Object> actualValues = new FileFormatRecommender().validValues(
                AzureBlobSinkConfig.FILE_FORMAT_CONF_KEY, new HashMap<>());

        Assertions.assertEquals(expectedValues, actualValues);
    }
}
