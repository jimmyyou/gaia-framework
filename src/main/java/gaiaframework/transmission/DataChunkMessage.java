package gaiaframework.transmission;

// The new DataChunk to replace the old one

import java.io.Serializable;

public class DataChunkMessage implements Serializable {

    private static final long serialVersionUID = 759099461767050471L;

    DataChunkHeader header;

    byte[] data;

    public DataChunkMessage(String filename, String destIP, String blockID, long startIndex, long totalBlockLength, byte[] data) {
        header = new DataChunkHeader(filename, destIP, blockID, startIndex, totalBlockLength);
        this.data = data;
    }

    public String getDestURL() { return header.getDestURL(); }

    public String getFilename() {
        return header.getFilename();
    }

    public String getBlockId() { return header.blockID; }

    public long getStartIndex() {
        return header.getChunkStartIndex();
    }

    public long getTotalBlockLength() { return header.getTotalBlockLength(); }

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
