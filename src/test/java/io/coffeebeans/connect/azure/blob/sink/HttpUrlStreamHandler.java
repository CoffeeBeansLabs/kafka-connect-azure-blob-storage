package io.coffeebeans.connect.azure.blob.sink;

import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom Http url stream handler to be used only in unit tests.
 */
public class HttpUrlStreamHandler extends URLStreamHandler {

    private final Map<URL, URLConnection> connections = new HashMap<>();

    @Override
    protected URLConnection openConnection(URL url) {
        return connections.get(url);
    }

    public void resetConnections() {
        connections.clear();
    }

    public HttpUrlStreamHandler addConnection(URL url, URLConnection urlConnection) {
        connections.put(url, urlConnection);
        return this;
    }
}
