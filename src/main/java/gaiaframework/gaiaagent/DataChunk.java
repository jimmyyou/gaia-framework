package gaiaframework.gaiaagent;

// Data Chunk to be transferred by agents

import gaiaframework.util.Constants;

import java.io.Serializable;

public class DataChunk implements Serializable {
    String filename;
    int startIndex;
    int chunkLength;
    byte[] data;

    public DataChunk(String filename, int startIndex, int chunkLength, byte[] data) {
        this.filename = filename;
        this.startIndex = startIndex;
        this.chunkLength = chunkLength;
        this.data = data;
    }

    public String getFilename() {
        return filename;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public int getChunkLength() {
        return chunkLength;
    }

    public byte[] getData() {
        return data;
    }

    public DataChunk(String filename) {
        this.filename = filename;

    }

}
