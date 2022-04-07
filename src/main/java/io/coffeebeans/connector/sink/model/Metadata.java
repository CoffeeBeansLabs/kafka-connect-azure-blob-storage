package io.coffeebeans.connector.sink.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Model to store the blob full path and the current index of the file in which is data is to be written.
 */
public class Metadata {

    @JsonProperty("fullPath")
    private String fullPath;

    @JsonProperty("index")
    private int index;

    public Metadata() {}

    public Metadata(String fullPath, int currentIndex) {
        this.fullPath = fullPath;
        this.index = currentIndex;
    }

    public String getFullPath() {
        return fullPath;
    }

    public void setFullPath(String fullPath) {
        this.fullPath = fullPath;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}