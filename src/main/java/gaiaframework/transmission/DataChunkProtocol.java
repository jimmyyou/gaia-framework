package gaiaframework.transmission;

// The new DataChunk to replace the old one

import java.io.Serializable;

public class DataChunkProtocol implements Serializable {
    String filename;
    String destURL;
    long startIndex;
    long chunkLength;

    byte[] data;

    public DataChunkProtocol(String filename, String destURL, long startIndex, long chunkLength, byte[] data) {
        this.filename = filename;
        this.destURL = destURL;
        this.startIndex = startIndex;
        this.chunkLength = chunkLength;
        this.data = data;
    }

    public String getDestURL() { return destURL; }

    public String getFilename() {
        return filename;
    }

    public long getStartIndex() {
        return startIndex;
    }

    public long getChunkLength() { return chunkLength; }

    public byte[] getData() {
        return data;
    }

    // for debug purpose only
    public void appendToFilename(String appendix) {
//        filename.concat(appendix);
        filename = filename + appendix;
    }
}
