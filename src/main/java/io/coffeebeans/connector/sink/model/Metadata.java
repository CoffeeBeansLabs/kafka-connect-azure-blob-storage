package io.coffeebeans.connector.sink.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Model to store the folder path and the current index of the file in which is data is being written.
 */
public class Metadata {

    @JsonProperty("folderPath")
    private String folderPath;

    @JsonProperty("index")
    private int index;

    public Metadata() {}

    public Metadata(String folderPath, int currentIndex) {
        this.folderPath = folderPath;
        this.index = currentIndex;
    }

    public String getFolderPath() {
        return folderPath;
    }

    public void setFolderPath(String folderPath) {
        this.folderPath = folderPath;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}