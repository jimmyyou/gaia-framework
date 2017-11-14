package gaiaframework.gaiaagent;

// Data Chunk to be transferred by agents

import java.io.Serializable;

public class DataChunk implements Serializable {
    String filename;
    long startIndex;
    long chunkLength;
    byte[] data;

    public DataChunk(String filename, long startIndex, long chunkLength, byte[] data) {
        this.filename = filename;
        this.startIndex = startIndex;
        this.chunkLength = chunkLength;
        this.data = data;
    }

    public String getFilename() {
        return filename;
    }

    public long getStartIndex() {
        return startIndex;
    }

    public long getChunkLength() {
        return chunkLength;
    }

    public byte[] getData() {
        return data;
    }

    public void appendToFilename(String appendix) {
//        filename.concat(appendix);
        filename = filename + appendix;
    }

/*    public DataChunk(String filename) {
        this.filename = filename;
    }*/

}
