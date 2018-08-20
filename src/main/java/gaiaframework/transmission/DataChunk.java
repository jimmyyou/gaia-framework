package gaiaframework.transmission;

// Data Chunk to be transferred by agents

// TODO(future): It would be better if we can make dataChunk be like a stream (can be easily divided into arbitrary size)

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
//        srcFilename.concat(appendix);
        filename = filename + appendix;
    }

/*    public DataChunk(String srcFilename) {
        this.srcFilename = srcFilename;
    }*/

}
