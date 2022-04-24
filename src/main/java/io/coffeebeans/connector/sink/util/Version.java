package io.coffeebeans.connector.sink.util;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will return the current version of the connector.
 */
public class Version {
    private static final Logger log = LoggerFactory.getLogger(Version.class);
    private static final String PROPERTIES_FILE_NAME = "/application.properties";
    private static final String VERSION_KEY = "version";
    private static final String UNKNOWN_VERSION = "unknown";
    private static final String VERSION;

    static {
        String versionProperty = UNKNOWN_VERSION;

        try {
            Properties properties = new Properties();
            properties.load(Version.class.getResourceAsStream(PROPERTIES_FILE_NAME));
            versionProperty = properties.getProperty(VERSION_KEY, versionProperty).trim();

        } catch (Exception e) {
            log.error("Error while loading version: " + e);

        } finally {
            VERSION = versionProperty;
        }
    }

    public static String getVersion() {
        return VERSION;
    }
}
