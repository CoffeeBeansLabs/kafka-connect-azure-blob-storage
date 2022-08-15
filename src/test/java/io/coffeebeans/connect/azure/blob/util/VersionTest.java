package io.coffeebeans.connect.azure.blob.util;

import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Version}.
 */
public class VersionTest {

    /**
     * <b>Method: {@link Version#getVersion()}</b>.
     */
    @Test
    public void getVersion_shouldReturnCorrectVersion() throws IOException {
        Properties properties = new Properties();
        properties.load(Version.class.getResourceAsStream("/application.properties"));
        String versionProperty = properties.getProperty("version").trim();

        Assertions.assertEquals(versionProperty, Version.getVersion());
    }
}
