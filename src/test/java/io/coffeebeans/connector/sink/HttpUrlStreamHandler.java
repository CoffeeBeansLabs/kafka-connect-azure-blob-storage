package io.coffeebeans.connector.sink;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.HashMap;
import java.util.Map;

public class HttpUrlStreamHandler extends URLStreamHandler {

    private final Map<URL, URLConnection> connections = new HashMap<>();

    @Override
    protected URLConnection openConnection(URL url) throws IOException {
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
