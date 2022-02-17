package io.coffeebeans.connector.sink.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

public class VersionTest {

    @Test
    public void test_getVersion() throws IOException {
        Properties properties = new Properties();
        properties.load(Version.class.getResourceAsStream("/application.properties"));
        String versionProperty = properties.getProperty("version").trim();

        Assertions.assertEquals(versionProperty, Version.getVersion());
    }
}
