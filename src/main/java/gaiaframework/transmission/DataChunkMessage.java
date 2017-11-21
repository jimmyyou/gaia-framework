package gaiaframework.transmission;

// The new DataChunk to replace the old one

import java.io.Serializable;
import java.util.Arrays;

public class DataChunkMessage implements Serializable {

    private static final long serialVersionUID = 759099461767050471L;

    DataChunkHeader header;

    byte[] data;

    public DataChunkMessage(String filename, String destURL, long startIndex, long chunkLength, byte[] data) {
        header = new DataChunkHeader(filename, destURL, startIndex, chunkLength);
        this.data = data;
    }

    public String getDestURL() { return header.getDestURL(); }

    public String getFilename() {
        return header.getFilename();
    }

    public long getStartIndex() {
        return header.getStartIndex();
    }

    public long getChunkLength() { return header.getChunkLength(); }

    public byte[] getData() {
        return data;
    }

    public DataChunkHeader getHeader() {
        return header;
    }

    public void setHeader(DataChunkHeader header) {
        this.header = header;
    }

    // for debug purpose only
    public void appendToFilename(String appendix) {
//        filename.concat(appendix);
        header.filename = header.filename + appendix;
    }

    @Override
    public String toString() {
        return "DataChunkMessage{" +
                "header=" + header +
                ", data size=" + data.length +
                '}';
    }
}
