package io.coffeebeans.connector.sink.util;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will return the current version of the connector.
 */
public class Version {
    private static final Logger log = LoggerFactory.getLogger(Version.class);
    private static final String VERSION;

    static {
        String versionProperty = "unknown";
        try {
            Properties properties = new Properties();
            properties.load(Version.class.getResourceAsStream("/application.properties"));
            versionProperty = properties.getProperty("version", versionProperty).trim();
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
